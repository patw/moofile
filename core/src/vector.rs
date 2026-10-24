//! Vector values: BSON arrays of numbers, and BSON Binary subtype 9.
//!
//! Subtype 9 is the BSON spec's packed vector (the format MongoDB's own
//! drivers write): one dtype byte, one padding byte, then the elements.
//!
//! | dtype | byte   | element                        |
//! |-------|--------|--------------------------------|
//! | INT8  | `0x03` | `i8`                           |
//! | FLOAT32 | `0x27` | little-endian `f32`          |
//! | PACKED_BIT | `0x10` | 1 bit, most significant first; the padding byte counts the unused low bits of the last byte |
//!
//! An array of doubles costs 13+ bytes per element on disk (type byte, a
//! decimal index key, eight bytes of value); FLOAT32 costs 4 and INT8 1.
//! Autoembedding writes subtype 9, and every vector index reads either form.

use bson::raw::RawBsonRef;
use bson::spec::BinarySubtype;
use bson::{Binary, Bson};

use crate::embed::EmbeddingPrecision;

pub(crate) const DTYPE_INT8: u8 = 0x03;
pub(crate) const DTYPE_FLOAT32: u8 = 0x27;
pub(crate) const DTYPE_PACKED_BIT: u8 = 0x10;

/// A validated view of a subtype-9 payload.
pub(crate) enum PackedVector<'a> {
    Int8(&'a [u8]),
    Float32(&'a [u8]),
    /// Packed bytes and the number of meaningful bits.
    PackedBit(&'a [u8], usize),
}

impl<'a> PackedVector<'a> {
    pub(crate) fn parse(bytes: &'a [u8]) -> Option<Self> {
        let (&dtype, rest) = bytes.split_first()?;
        let (&padding, data) = rest.split_first()?;
        match dtype {
            DTYPE_INT8 if padding == 0 => Some(Self::Int8(data)),
            DTYPE_FLOAT32 if padding == 0 && data.len() % 4 == 0 => Some(Self::Float32(data)),
            DTYPE_PACKED_BIT if padding < 8 && (padding == 0 || !data.is_empty()) => {
                Some(Self::PackedBit(data, data.len() * 8 - padding as usize))
            }
            _ => None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Int8(d) => d.len(),
            Self::Float32(d) => d.len() / 4,
            Self::PackedBit(_, n) => *n,
        }
    }

    /// The elements as `f32`.  Packed bits map to ±1 so that cosine
    /// similarity between bit vectors is the Hamming-derived score.
    pub(crate) fn to_f32(&self) -> Vec<f32> {
        match self {
            Self::Int8(d) => d.iter().map(|&b| b as i8 as f32).collect(),
            Self::Float32(d) => d
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect(),
            Self::PackedBit(d, n) => (0..*n)
                .map(|i| if d[i / 8] & (0x80 >> (i % 8)) != 0 { 1.0 } else { -1.0 })
                .collect(),
        }
    }

    /// The elements as BSON numbers — what a JSON consumer gets: floats for
    /// FLOAT32, integers for INT8, 0/1 for packed bits.
    pub(crate) fn to_bson_numbers(&self) -> Vec<Bson> {
        match self {
            Self::Int8(d) => d.iter().map(|&b| Bson::Int32(b as i8 as i32)).collect(),
            Self::Float32(_) => self.to_f32().into_iter().map(|v| Bson::Double(v as f64)).collect(),
            Self::PackedBit(d, n) => (0..*n)
                .map(|i| Bson::Int32((d[i / 8] & (0x80 >> (i % 8)) != 0) as i32))
                .collect(),
        }
    }
}

/// Read a stored vector field as `f32`: an array of numbers or a subtype-9 binary.
/// `None` for anything else, including an array with a non-numeric element
/// (such an array used to be indexed with the bad elements silently dropped,
/// which shifted every later dimension).
pub(crate) fn raw_vector(val: RawBsonRef) -> Option<Vec<f32>> {
    match val {
        RawBsonRef::Array(arr) => arr
            .into_iter()
            .map(|v| match v.ok()? {
                RawBsonRef::Double(f) => Some(f as f32),
                RawBsonRef::Int32(i) => Some(i as f32),
                RawBsonRef::Int64(i) => Some(i as f32),
                _ => None,
            })
            .collect(),
        RawBsonRef::Binary(b) if b.subtype == BinarySubtype::Vector => {
            Some(PackedVector::parse(b.bytes)?.to_f32())
        }
        _ => None,
    }
}

/// A stored vector in the form a vector index keeps it.  INT8 and PACKED_BIT
/// stay quantised instead of being widened to `f32`: the widened values are
/// exactly the codes (or ±1), so nothing is gained by widening them and the
/// matrix would be 4× (or 32×) larger.
pub(crate) enum StoredVector {
    F32(Vec<f32>),
    Int8(Vec<i8>),
    /// Element `i` is bit `i % 64` of word `i / 64`; the unused high bits of
    /// the last word are zero.
    Bits(Vec<u64>, usize),
}

impl StoredVector {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::F32(v) => v.len(),
            Self::Int8(v) => v.len(),
            Self::Bits(_, n) => *n,
        }
    }
}

/// [`raw_vector`], keeping INT8 and PACKED_BIT quantised.
pub(crate) fn raw_stored_vector(val: RawBsonRef) -> Option<StoredVector> {
    if let RawBsonRef::Binary(b) = val {
        if b.subtype == BinarySubtype::Vector {
            return Some(match PackedVector::parse(b.bytes)? {
                PackedVector::Int8(d) => StoredVector::Int8(d.iter().map(|&b| b as i8).collect()),
                PackedVector::PackedBit(d, n) => StoredVector::Bits(pack_bits(n, |i| d[i / 8] & (0x80 >> (i % 8)) != 0), n),
                f @ PackedVector::Float32(_) => StoredVector::F32(f.to_f32()),
            });
        }
    }
    raw_vector(val).map(StoredVector::F32)
}

/// Pack `n` bits, element `i` into bit `i % 64` of word `i / 64`.
pub(crate) fn pack_bits(n: usize, bit: impl Fn(usize) -> bool) -> Vec<u64> {
    let mut words = vec![0u64; n.div_ceil(64)];
    for i in (0..n).filter(|&i| bit(i)) {
        words[i / 64] |= 1 << (i % 64);
    }
    words
}

/// The element count of a stored vector, without decoding it.
pub(crate) fn raw_vector_len(val: RawBsonRef) -> Option<usize> {
    match val {
        // RawArray has no O(1) len(): arrays share the document wire
        // format, so this walks the raw bytes once.
        RawBsonRef::Array(arr) => Some(arr.into_iter().count()),
        RawBsonRef::Binary(b) if b.subtype == BinarySubtype::Vector => {
            Some(PackedVector::parse(b.bytes)?.len())
        }
        _ => None,
    }
}

/// Encode an embedding for storage at the configured precision.
///
/// `int8` keeps the symmetric quantisation's integers (scale is irrelevant
/// to cosine).  `uint8` has no subtype-9 dtype; its min-max codes are stored
/// re-centred as INT8 (`code - 128`), which is exactly the vector search
/// used to see after dequantising them.  `binary` packs sign bits.
pub(crate) fn encode_embedding(emb: &[f32], precision: EmbeddingPrecision) -> Bson {
    let quantized = crate::embed::quantize(emb, precision);
    let (dtype, padding, data) = match precision {
        EmbeddingPrecision::F32 => (DTYPE_FLOAT32, 0, quantized),
        EmbeddingPrecision::Int8 => (DTYPE_INT8, 0, quantized),
        EmbeddingPrecision::Uint8 => (
            DTYPE_INT8,
            0,
            quantized.iter().map(|&b| (b as i16 - 128) as i8 as u8).collect(),
        ),
        EmbeddingPrecision::Binary => {
            // quantize() packs least-significant-bit first; subtype 9 is
            // most-significant first.
            let data = quantized.iter().map(|b| b.reverse_bits()).collect();
            (DTYPE_PACKED_BIT, ((8 - emb.len() % 8) % 8) as u8, data)
        }
    };
    let mut bytes = Vec::with_capacity(2 + data.len());
    bytes.push(dtype);
    bytes.push(padding);
    bytes.extend_from_slice(&data);
    Bson::Binary(Binary { subtype: BinarySubtype::Vector, bytes })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(emb: &[f32], p: EmbeddingPrecision) -> Vec<f32> {
        let v = encode_embedding(emb, p);
        assert!(matches!(&v, Bson::Binary(b) if b.subtype == BinarySubtype::Vector));
        let doc = bson::RawDocumentBuf::from_document(&bson::doc! { "v": v }).unwrap();
        raw_vector(doc.get("v").unwrap().unwrap()).unwrap()
    }

    #[test]
    fn float32_is_lossless_and_four_bytes_per_dim() {
        let emb = [0.5f32, -0.25, 1.0];
        let Bson::Binary(b) = encode_embedding(&emb, EmbeddingPrecision::F32) else { panic!() };
        assert_eq!(b.bytes.len(), 2 + 12);
        assert_eq!(roundtrip(&emb, EmbeddingPrecision::F32), emb);
    }

    #[test]
    fn int8_is_one_byte_per_dim() {
        let emb = [0.5f32, -1.0, 0.25];
        let Bson::Binary(b) = encode_embedding(&emb, EmbeddingPrecision::Int8) else { panic!() };
        assert_eq!(b.bytes.len(), 2 + 3);
        assert_eq!(roundtrip(&emb, EmbeddingPrecision::Int8), [64.0, -127.0, 32.0]);
    }

    #[test]
    fn uint8_recentres_to_what_search_used_to_see() {
        let emb = [0.0f32, 1.0, 0.5];
        let codes = crate::embed::quantize(&emb, EmbeddingPrecision::Uint8);
        let old: Vec<f32> = crate::embed::dequantize(&codes, EmbeddingPrecision::Uint8, 3);
        let new = roundtrip(&emb, EmbeddingPrecision::Uint8);
        for (o, n) in old.iter().zip(&new) {
            assert!((o * 128.0 - n).abs() < 1e-4);
        }
    }

    #[test]
    fn binary_packs_msb_first_with_padding() {
        let emb = [1.0f32, -1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0, 1.0, -1.0];
        let Bson::Binary(b) = encode_embedding(&emb, EmbeddingPrecision::Binary) else { panic!() };
        assert_eq!(b.bytes, vec![DTYPE_PACKED_BIT, 6, 0b1011_0000, 0b1000_0000]);
        assert_eq!(roundtrip(&emb, EmbeddingPrecision::Binary), emb);
    }

    #[test]
    fn malformed_payloads_are_rejected() {
        assert!(PackedVector::parse(&[]).is_none());
        assert!(PackedVector::parse(&[DTYPE_FLOAT32, 0, 1, 2, 3]).is_none());
        assert!(PackedVector::parse(&[DTYPE_INT8, 1, 5]).is_none());
        assert!(PackedVector::parse(&[DTYPE_PACKED_BIT, 3]).is_none());
        assert!(PackedVector::parse(&[0x99, 0, 1]).is_none());
    }

    #[test]
    fn arrays_with_a_non_number_are_not_vectors() {
        let doc = bson::rawdoc! { "v": [1.0, "x", 2.0] };
        assert!(raw_vector(doc.get("v").unwrap().unwrap()).is_none());
    }
}
