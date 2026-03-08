"""Text search with BM25 scoring and Porter stemming."""

import math
import re
from collections import defaultdict
from typing import Dict, List, Tuple

from snowballstemmer import stemmer


class TextIndex:
    """
    In-memory text index with BM25 scoring.
    
    Uses Porter stemming and maintains inverted indexes for fast text search.
    """
    
    def __init__(self, k1: float = 1.2, b: float = 0.75):
        """
        Initialize text index.
        
        Args:
            k1: BM25 term frequency saturation parameter
            b: BM25 field length normalization parameter  
        """
        self.k1 = k1
        self.b = b
        self._stemmer = stemmer('english')
        
        # Inverted index: stem -> {doc_id -> term_frequency}
        self._inverted_index: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # Document metadata
        self._doc_lengths: Dict[str, int] = {}  # doc_id -> total_terms
        self._doc_count = 0
        self._total_length = 0
        
        # Document frequency: stem -> number of documents containing it
        self._doc_frequencies: Dict[str, int] = defaultdict(int)
    
    def _tokenize_and_stem(self, text: str) -> List[str]:
        """Tokenize text and apply stemming."""
        # Simple tokenization: split on non-alphanumeric, lowercase
        tokens = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        # Apply stemming
        return [self._stemmer.stemWord(token) for token in tokens if len(token) > 1]
    
    def add_document(self, doc_id: str, text: str) -> None:
        """Add a document to the text index."""
        if doc_id in self._doc_lengths:
            # Document already exists, remove it first
            self.remove_document(doc_id)
        
        stems = self._tokenize_and_stem(text)
        if not stems:
            return
        
        # Count term frequencies
        term_freqs = defaultdict(int)
        for stem in stems:
            term_freqs[stem] += 1
        
        # Add to inverted index
        for stem, freq in term_freqs.items():
            self._inverted_index[stem][doc_id] = freq
            if doc_id not in [d for d in self._inverted_index[stem].keys()]:
                self._doc_frequencies[stem] += 1
        
        # Update document metadata
        self._doc_lengths[doc_id] = len(stems)
        self._doc_count += 1
        self._total_length += len(stems)
        
        # Update document frequencies
        for stem in term_freqs.keys():
            if stem not in self._doc_frequencies:
                self._doc_frequencies[stem] = 0
            # Count unique documents containing this stem
            docs_with_stem = len(self._inverted_index[stem])
            self._doc_frequencies[stem] = docs_with_stem
    
    def remove_document(self, doc_id: str) -> None:
        """Remove a document from the text index."""
        if doc_id not in self._doc_lengths:
            return
        
        doc_length = self._doc_lengths.pop(doc_id)
        self._doc_count -= 1
        self._total_length -= doc_length
        
        # Remove from inverted index and update doc frequencies
        stems_to_remove = []
        for stem, doc_freqs in self._inverted_index.items():
            if doc_id in doc_freqs:
                del doc_freqs[doc_id]
                self._doc_frequencies[stem] = len(doc_freqs)
                if not doc_freqs:
                    stems_to_remove.append(stem)
        
        # Clean up empty stems
        for stem in stems_to_remove:
            del self._inverted_index[stem]
            del self._doc_frequencies[stem]
    
    def search(self, query: str, limit: int = 10) -> List[Tuple[str, float]]:
        """
        Search for documents matching the query using BM25 scoring.
        
        Returns:
            List of (doc_id, score) tuples sorted by score descending
        """
        if self._doc_count == 0:
            return []
        
        query_stems = self._tokenize_and_stem(query)
        if not query_stems:
            return []
        
        # Calculate average document length
        avg_doc_length = self._total_length / self._doc_count if self._doc_count > 0 else 0
        
        # Calculate BM25 scores for each document
        doc_scores: Dict[str, float] = defaultdict(float)
        
        for stem in query_stems:
            if stem not in self._inverted_index:
                continue
            
            # Document frequency for this stem
            df = self._doc_frequencies[stem]
            
            # IDF calculation: log((N - df + 0.5) / (df + 0.5))
            idf = math.log((self._doc_count - df + 0.5) / (df + 0.5))
            
            # Score each document containing this stem
            for doc_id, tf in self._inverted_index[stem].items():
                doc_length = self._doc_lengths[doc_id]
                
                # BM25 term frequency component
                tf_component = (tf * (self.k1 + 1)) / (
                    tf + self.k1 * (1 - self.b + self.b * (doc_length / avg_doc_length))
                )
                
                # Add to document score
                doc_scores[doc_id] += idf * tf_component
        
        # Sort by score descending and limit results
        sorted_results = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_results[:limit]
    
    def clear(self) -> None:
        """Clear all documents and indexes."""
        self._inverted_index.clear()
        self._doc_lengths.clear()
        self._doc_frequencies.clear()
        self._doc_count = 0
        self._total_length = 0