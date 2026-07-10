/// BM25 text search with Porter stemming.
///
/// Uses the `rust-stemmers` crate (which wraps the same C Snowball
/// library as Python's `snowballstemmer`), so stemming behaviour
/// should be identical between implementations.

use std::collections::HashMap;

use rust_stemmers::{Algorithm, Stemmer};

// ---------------------------------------------------------------------------
// TextIndex
// ---------------------------------------------------------------------------

/// In-memory inverted index for full-text search.
///
/// Tokenizes documents with a simple regex (`[a-zA-Z]+`), lowercases,
/// stems with Porter, and scores with BM25.
pub(crate) struct TextIndex {
    k1: f32,
    b: f32,
    stemmer: Stemmer,

    /// Inverted index: stem → (doc_id → term_frequency)
    inverted: HashMap<String, HashMap<String, u32>>,

    /// Document metadata
    doc_lengths: HashMap<String, u32>, // doc_id → total terms
    total_length: u64,

    /// Document frequency: stem → number of documents containing it
    doc_frequencies: HashMap<String, u32>,
}

impl std::fmt::Debug for TextIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextIndex")
            .field("k1", &self.k1)
            .field("b", &self.b)
            .field("ndocs", &self.doc_lengths.len())
            .field("nterms", &self.inverted.len())
            .finish()
    }
}

impl TextIndex {
    pub fn new() -> Self {
        Self {
            k1: 1.2,
            b: 0.75,
            stemmer: Stemmer::create(Algorithm::English),
            inverted: HashMap::new(),
            doc_lengths: HashMap::new(),
            total_length: 0,
            doc_frequencies: HashMap::new(),
        }
    }

    // ---------------------------------------------------------------
    // Document management
    // ---------------------------------------------------------------

    pub fn add_document(&mut self, doc_id: String, text: &str) {
        // Remove old version if present
        self.remove_document(&doc_id);

        let stems = tokenize_and_stem(&self.stemmer, text);
        if stems.is_empty() {
            return;
        }

        // Count term frequencies
        let mut term_freqs: HashMap<String, u32> = HashMap::new();
        for stem in &stems {
            *term_freqs.entry(stem.clone()).or_default() += 1;
        }

        // Add to inverted index
        for (stem, freq) in &term_freqs {
            self.inverted
                .entry(stem.clone())
                .or_default()
                .insert(doc_id.clone(), *freq);
        }

        // Update document metadata
        let len = stems.len() as u32;
        self.doc_lengths.insert(doc_id.clone(), len);
        self.total_length += len as u64;

        // Update document frequencies
        for stem in term_freqs.keys() {
            let df = self.inverted.get(stem).map(|m| m.len() as u32).unwrap_or(0);
            self.doc_frequencies.insert(stem.clone(), df);
        }
    }

    pub fn remove_document(&mut self, doc_id: &str) {
        let len = match self.doc_lengths.remove(doc_id) {
            Some(l) => l,
            None => return,
        };

        self.total_length -= len as u64;

        // Remove from inverted index
        let mut empty_stems = Vec::new();
        for (stem, docs) in self.inverted.iter_mut() {
            if docs.remove(doc_id).is_some() {
                self.doc_frequencies.insert(stem.clone(), docs.len() as u32);
                if docs.is_empty() {
                    empty_stems.push(stem.clone());
                }
            }
        }

        for stem in &empty_stems {
            self.inverted.remove(stem);
            self.doc_frequencies.remove(stem);
        }
    }

    // ---------------------------------------------------------------
    // Search
    // ---------------------------------------------------------------

    /// BM25 search.  Returns `(doc_id, score)` pairs sorted descending.
    pub fn search(&self, query: &str, limit: usize) -> Vec<(String, f32)> {
        let n_docs = self.doc_lengths.len();
        if n_docs == 0 {
            return Vec::new();
        }

        let query_stems = tokenize_and_stem(&self.stemmer, query);
        if query_stems.is_empty() {
            return Vec::new();
        }

        let avgdl = self.total_length as f32 / n_docs as f32;

        let mut doc_scores: HashMap<String, f32> = HashMap::new();

        for stem in &query_stems {
            let docs = match self.inverted.get(stem) {
                Some(d) => d,
                None => continue,
            };

            let df = self.doc_frequencies.get(stem).copied().unwrap_or(0) as f32;
            let idf = ((n_docs as f32 - df + 0.5) / (df + 0.5)).ln();

            for (doc_id, tf) in docs {
                let dl = *self.doc_lengths.get(doc_id).unwrap_or(&1) as f32;
                let tf_component =
                    (*tf as f32 * (self.k1 + 1.0))
                        / (*tf as f32 + self.k1 * (1.0 - self.b + self.b * (dl / avgdl)));

                *doc_scores.entry(doc_id.clone()).or_default() += idf * tf_component;
            }
        }

        let mut scored: Vec<(String, f32)> = doc_scores.into_iter().collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored.truncate(limit);
        scored
    }

    pub fn clear(&mut self) {
        self.inverted.clear();
        self.doc_lengths.clear();
        self.doc_frequencies.clear();
        self.total_length = 0;
    }
}

// ---------------------------------------------------------------------------
// Tokenization
// ---------------------------------------------------------------------------

fn tokenize_and_stem(stemmer: &Stemmer, text: &str) -> Vec<String> {
    // Simple regex: match runs of ASCII letters.
    let re = regex_lite::Regex::new(r"[a-zA-Z]+").unwrap();
    re.find_iter(text)
        .map(|m| m.as_str().to_lowercase())
        .filter(|t| t.len() > 1)
        .map(|t| stemmer.stem(&t).into_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stemming_english() {
        let stemmer = Stemmer::create(Algorithm::English);
        // "running" → "run", "cats" → "cat"
        assert_eq!(stemmer.stem("running").into_owned(), "run");
        assert_eq!(stemmer.stem("cats").into_owned(), "cat");
    }

    #[test]
    fn add_and_search() {
        let mut ti = TextIndex::new();
        ti.add_document("1".into(), "machine learning is great");
        ti.add_document("2".into(), "deep learning and machine learning");
        ti.add_document("3".into(), "cooking recipes for dinner");

        let results = ti.search("machine learning", 10);
        assert!(!results.is_empty());

        // Both docs are about ML; doc 2 has an extra "learning".
        // Due to BM25 length normalization the ordering depends on
        // the exact tf-idf balance.  Either "1" or "2" is valid.
        let top_ids: Vec<&str> = results.iter().map(|(id, _)| id.as_str()).collect();
        assert!(top_ids.contains(&"1"));
        assert!(top_ids.contains(&"2"));
    }

    #[test]
    fn remove_document() {
        let mut ti = TextIndex::new();
        ti.add_document("a".into(), "hello world");
        ti.add_document("b".into(), "hello rust");
        assert_eq!(ti.search("hello", 10).len(), 2);

        ti.remove_document("a");
        let results = ti.search("hello", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "b");
    }

    #[test]
    fn empty_index() {
        let ti = TextIndex::new();
        assert!(ti.search("anything", 10).is_empty());
    }
}
