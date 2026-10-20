/// BM25 text search with Porter stemming.
///
/// Uses the `rust-stemmers` crate (which wraps the same C Snowball
/// library as Python's `snowballstemmer`), so stemming behaviour
/// should be identical between implementations.

use std::collections::HashMap;

use rust_stemmers::{Algorithm, Stemmer};

use crate::index::Row;

// ---------------------------------------------------------------------------
// TextIndex
// ---------------------------------------------------------------------------

/// In-memory inverted index for full-text search.
///
/// Tokenizes documents with a simple regex (`[a-zA-Z]+`), lowercases,
/// stems with Porter, and scores with BM25.  Documents are identified by
/// their [`Row`]; see `index.rs`.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct TextIndex {
    k1: f32,
    b: f32,
    #[serde(skip, default = "default_stemmer")]
    stemmer: Stemmer,

    /// Inverted index: stem → (row → term frequency).  A stem's document
    /// frequency is its posting count; it used to be duplicated in a second
    /// map keyed by stem.
    inverted: HashMap<String, HashMap<Row, u32>>,

    /// Row → total terms in the document; 0 means "not indexed" (an indexed
    /// document always has at least one term).
    lengths: Vec<u32>,
    n_docs: usize,
    total_length: u64,
}

impl Clone for TextIndex {
    fn clone(&self) -> Self {
        Self {
            k1: self.k1,
            b: self.b,
            stemmer: Stemmer::create(Algorithm::English),
            inverted: self.inverted.clone(),
            lengths: self.lengths.clone(),
            n_docs: self.n_docs,
            total_length: self.total_length,
        }
    }
}

/// Create an English Porter stemmer — used as the serde default for the
/// non-serializable `Stemmer` field when deserializing from cache.
fn default_stemmer() -> Stemmer {
    Stemmer::create(Algorithm::English)
}

impl std::fmt::Debug for TextIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextIndex")
            .field("k1", &self.k1)
            .field("b", &self.b)
            .field("ndocs", &self.n_docs)
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
            lengths: Vec::new(),
            n_docs: 0,
            total_length: 0,
        }
    }

    fn length(&self, row: Row) -> u32 {
        self.lengths.get(row as usize).copied().unwrap_or(0)
    }

    // ---------------------------------------------------------------
    // Document management
    // ---------------------------------------------------------------

    pub fn add_document(&mut self, row: Row, text: &str) {
        // Rows are only reused after removal, so this is a guard, not a path.
        if self.length(row) != 0 {
            self.purge(row);
        }

        let stems = tokenize_and_stem(&self.stemmer, text);
        if stems.is_empty() {
            return;
        }

        let mut term_freqs: HashMap<String, u32> = HashMap::new();
        for stem in stems.iter() {
            *term_freqs.entry(stem.clone()).or_default() += 1;
        }
        for (stem, freq) in term_freqs {
            self.inverted.entry(stem).or_default().insert(row, freq);
        }

        let len = stems.len() as u32;
        if self.lengths.len() <= row as usize {
            self.lengths.resize(row as usize + 1, 0);
        }
        self.lengths[row as usize] = len;
        self.n_docs += 1;
        self.total_length += len as u64;
    }

    /// Remove a document, given the text it was indexed from.
    ///
    /// Re-tokenising that text names exactly the stems holding a posting for
    /// this row, so removal costs O(terms in the document).  It used to walk
    /// every stem in the vocabulary — the reason update and delete slowed as
    /// a collection's vocabulary grew.  If the text does not account for
    /// every term recorded at add time (it always should: the caller passes
    /// the same stored bytes) the full walk still runs, so a stale posting
    /// can never survive.
    pub fn remove_document(&mut self, row: Row, text: Option<&str>) {
        let len = self.length(row);
        if len == 0 {
            return;
        }
        let mut removed: u64 = 0;
        if let Some(text) = text {
            let mut stems = tokenize_and_stem(&self.stemmer, text);
            stems.sort_unstable();
            stems.dedup();
            for stem in &stems {
                if let Some(docs) = self.inverted.get_mut(stem) {
                    if let Some(tf) = docs.remove(&row) {
                        removed += tf as u64;
                        if docs.is_empty() {
                            self.inverted.remove(stem);
                        }
                    }
                }
            }
        }
        if removed != len as u64 {
            self.purge_postings(row);
        }
        self.lengths[row as usize] = 0;
        self.n_docs -= 1;
        self.total_length -= len as u64;
    }

    fn purge(&mut self, row: Row) {
        let len = self.length(row);
        self.purge_postings(row);
        self.lengths[row as usize] = 0;
        self.n_docs -= 1;
        self.total_length -= len as u64;
    }

    fn purge_postings(&mut self, row: Row) {
        self.inverted.retain(|_, docs| {
            docs.remove(&row);
            !docs.is_empty()
        });
    }

    // ---------------------------------------------------------------
    // Search
    // ---------------------------------------------------------------

    /// BM25 score of every document matching `query`, unordered.  Ranking
    /// and the `_id` tie-break happen in `IndexManager`, which can see ids.
    pub fn scores(&self, query: &str) -> Vec<(Row, f32)> {
        let n_docs = self.n_docs;
        if n_docs == 0 {
            return Vec::new();
        }

        let query_stems = tokenize_and_stem(&self.stemmer, query);
        if query_stems.is_empty() {
            return Vec::new();
        }

        let avgdl = self.total_length as f32 / n_docs as f32;

        let mut doc_scores: HashMap<Row, f32> = HashMap::new();

        for stem in &query_stems {
            let docs = match self.inverted.get(stem) {
                Some(d) => d,
                None => continue,
            };

            let df = docs.len() as f32;
            // The Lucene form, always positive. The unsmoothed
            // ln((N - df + 0.5) / (df + 0.5)) goes negative once a term is in
            // more than half the documents, and a negative IDF inverts the
            // ranking: every extra occurrence of the term pushes a document
            // *down*.  Must match moofile/text_search.py.
            let idf = (1.0 + (n_docs as f32 - df + 0.5) / (df + 0.5)).ln();

            for (&row, tf) in docs {
                let dl = self.length(row).max(1) as f32;
                let tf_component =
                    (*tf as f32 * (self.k1 + 1.0))
                        / (*tf as f32 + self.k1 * (1.0 - self.b + self.b * (dl / avgdl)));

                *doc_scores.entry(row).or_default() += idf * tf_component;
            }
        }

        doc_scores.into_iter().collect()
    }

    pub fn clear(&mut self) {
        self.inverted.clear();
        self.lengths.clear();
        self.n_docs = 0;
        self.total_length = 0;
    }
}

// ---------------------------------------------------------------------------
// Tokenization
// ---------------------------------------------------------------------------

fn tokenize_and_stem(stemmer: &Stemmer, text: &str) -> Vec<String> {
    // Compiled once: this runs for every document indexed and every query,
    // and recompiling the regex each call dominated tokenisation cost.
    //
    // The pattern keeps digits and holds compound identifiers together across
    // internal `.`, `-` and `_`, so `v1.2.0`, `595.71.05`, `14900K`,
    // `llama.cpp` and `x86-64-v3` survive as single terms.  An earlier
    // `[a-zA-Z]+` dropped every digit, which made version strings, driver
    // revisions and part numbers unsearchable -- and made two different
    // versions of the same product tokenize identically.
    static TOKEN_RE: std::sync::OnceLock<regex_lite::Regex> = std::sync::OnceLock::new();
    let re = TOKEN_RE.get_or_init(|| {
        regex_lite::Regex::new(r"[a-zA-Z0-9]+(?:[._-][a-zA-Z0-9]+)*").unwrap()
    });

    let mut out = Vec::new();
    for m in re.find_iter(text) {
        let whole = m.as_str().to_lowercase();

        // A compound also yields its parts, so `llama.cpp` is reachable by
        // `llama`, and `x86-64-v3` by `x86` or `v3`.  This also preserves the
        // pre-1.2.2 behaviour for alphabetic compounds, which the old regex
        // split on punctuation.
        let has_sep = whole.contains(['.', '-', '_']);
        if has_sep {
            for part in whole.split(['.', '-', '_']).filter(|p| p.len() > 1) {
                out.push(stem_term(stemmer, part));
            }
        }

        if whole.len() > 1 {
            out.push(stem_term(stemmer, &whole));
        }
    }
    out
}

/// Stem alphabetic terms only.  Porter is defined over words; running it on
/// `14900k` or `1.2.0` mangles identifiers to no benefit.
fn stem_term(stemmer: &Stemmer, term: &str) -> String {
    if term.bytes().all(|b| b.is_ascii_alphabetic()) {
        stemmer.stem(term).into_owned()
    } else {
        term.to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// All matches, best first, ties by row.
    fn search(ti: &TextIndex, q: &str) -> Vec<(Row, f32)> {
        let mut s = ti.scores(q);
        s.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        s
    }

    #[test]
    fn digits_are_indexed_and_discriminate() {
        let mut ti = TextIndex::new();
        ti.add_document(0, "moofile v1.2.0 released with voyage-4-nano as the default");
        ti.add_document(1, "moofile v0.3.0 rust core rewrite monorepo pyo3");
        ti.add_document(2, "behopc upgraded to the Intel 14900K arrow lake cpu");
        ti.add_document(3, "nvidia driver 595.71.05 suspend resume monitors stay black");

        // Bare identifiers used to tokenize to nothing at all.
        assert_eq!(search(&ti, "14900K")[0].0, 2);
        assert_eq!(search(&ti, "595.71.05")[0].0, 3);

        // Two versions of the same product must not rank identically.
        assert_eq!(search(&ti, "moofile v1.2.0")[0].0, 0);
        assert_eq!(search(&ti, "moofile v0.3.0")[0].0, 1);
    }

    #[test]
    fn compounds_are_reachable_by_their_parts() {
        let mut ti = TextIndex::new();
        ti.add_document(0, "built llama.cpp with CUDA on behoserver");
        ti.add_document(1, "behothinkpad is x86-64-v3 compatible, not v4");

        // The whole compound, and each part of it.
        assert_eq!(search(&ti, "llama.cpp")[0].0, 0);
        assert_eq!(search(&ti, "llama")[0].0, 0);
        assert_eq!(search(&ti, "x86-64-v3")[0].0, 1);
        assert_eq!(search(&ti, "x86")[0].0, 1);
    }

    #[test]
    fn digit_bearing_terms_are_not_stemmed() {
        let stemmer = Stemmer::create(Algorithm::English);
        // Porter is defined over words; identifiers must pass through intact.
        assert_eq!(stem_term(&stemmer, "14900k"), "14900k");
        assert_eq!(stem_term(&stemmer, "1.2.0"), "1.2.0");
        // ...while ordinary words still stem.
        assert_eq!(stem_term(&stemmer, "running"), "run");
    }

    #[test]
    fn single_characters_are_still_dropped() {
        let stemmer = Stemmer::create(Algorithm::English);
        let toks = tokenize_and_stem(&stemmer, "a b 5 of it");
        assert!(toks.iter().all(|t| t.len() > 1), "got {toks:?}");
    }

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
        ti.add_document(1, "machine learning is great");
        ti.add_document(2, "deep learning and machine learning");
        ti.add_document(3, "cooking recipes for dinner");

        let results = search(&ti, "machine learning");

        // Both docs are about ML; doc 2 has an extra "learning", which must
        // win.  "learning" is in 2 of 3 docs, and under the old unsmoothed
        // IDF that made its weight negative, so the extra occurrence *lost* --
        // this test used to accept either order to paper over it.
        let top: Vec<Row> = results.iter().map(|(row, _)| *row).collect();
        assert_eq!(top, vec![2, 1]);
        assert!(results.iter().all(|(_, score)| *score > 0.0));
    }

    #[test]
    fn idf_stays_positive_for_a_term_in_every_doc() {
        let mut ti = TextIndex::new();
        ti.add_document(0, "deploy redis");
        ti.add_document(1, "deploy notes from the long meeting today");

        let results = search(&ti, "deploy");
        assert!(results.iter().all(|(_, score)| *score > 0.0), "{results:?}");
        // Same tf; the shorter document is the more focused match.
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn remove_document() {
        let mut ti = TextIndex::new();
        ti.add_document(0, "hello world");
        ti.add_document(1, "hello rust");
        assert_eq!(search(&ti, "hello").len(), 2);

        ti.remove_document(0, Some("hello world"));
        let results = search(&ti, "hello");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
        assert!(search(&ti, "world").is_empty(), "a stem only row 0 had must be gone");
        assert_eq!((ti.n_docs, ti.total_length), (1, 2));
    }

    /// Removal is driven by the document's text; if that text is missing or
    /// does not account for the indexed terms, the full walk still runs.
    #[test]
    fn remove_falls_back_when_the_text_disagrees() {
        for text in [None, Some("something else entirely"), Some("hello")] {
            let mut ti = TextIndex::new();
            ti.add_document(0, "hello world");
            ti.add_document(1, "hello rust");
            ti.remove_document(0, text);
            assert_eq!(search(&ti, "hello").iter().map(|r| r.0).collect::<Vec<_>>(), [1]);
            assert!(search(&ti, "world").is_empty(), "text {text:?} left a posting behind");
            assert_eq!((ti.n_docs, ti.total_length), (1, 2));
        }
    }

    #[test]
    fn repeated_terms_are_removed_with_their_full_count() {
        let mut ti = TextIndex::new();
        ti.add_document(4, "deploy deploy deploy redis");
        ti.remove_document(4, Some("deploy deploy deploy redis"));
        assert!(ti.inverted.is_empty());
        assert_eq!((ti.n_docs, ti.total_length), (0, 0));
    }

    #[test]
    fn empty_index() {
        let ti = TextIndex::new();
        assert!(search(&ti, "anything").is_empty());
    }
}
