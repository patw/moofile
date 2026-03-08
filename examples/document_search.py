"""
Document Search Engine Example.

Demonstrates MooFile v0.2.0's vector similarity search and BM25 text search
for building a semantic + keyword document search system. Shows how to:

- Store documents with content embeddings and metadata
- Perform semantic search using vector similarity  
- Perform keyword search using BM25 text search
- Combine both approaches for hybrid search
- Use filters to narrow search scope
"""

import os
import tempfile
import numpy as np
from moofile import Collection

# Simulate document embeddings (in practice, these would come from a model like BERT/OpenAI)
def get_embedding(text: str) -> list:
    """Generate a fake embedding for demo purposes."""
    # In reality, you'd use a real embedding model
    np.random.seed(hash(text) % 2**32)  # Deterministic based on text
    return np.random.normal(0, 1, 384).tolist()  # 384-dimensional vector


def seed_documents(db: Collection) -> None:
    """Load sample documents into the database."""
    
    documents = [
        {
            "title": "Introduction to Machine Learning",
            "content": "Machine learning is a subset of artificial intelligence that enables computers to learn and make decisions from data without explicit programming. Key techniques include supervised learning, unsupervised learning, and reinforcement learning.",
            "author": "Dr. Sarah Chen",
            "category": "AI/ML",
            "tags": ["machine learning", "artificial intelligence", "data science"],
            "year": 2023,
            "citation_count": 45
        },
        {
            "title": "Deep Neural Networks for Computer Vision", 
            "content": "Convolutional neural networks have revolutionized computer vision tasks. This paper explores advanced architectures like ResNet, DenseNet, and Vision Transformers for image classification and object detection.",
            "author": "Prof. Michael Zhang",
            "category": "Computer Vision",
            "tags": ["neural networks", "computer vision", "CNN", "transformers"],
            "year": 2022,
            "citation_count": 123
        },
        {
            "title": "Natural Language Processing with Transformers",
            "content": "Transformer architectures like BERT, GPT, and T5 have achieved state-of-the-art results in natural language understanding tasks including sentiment analysis, question answering, and text generation.",
            "author": "Dr. Emily Rodriguez", 
            "category": "NLP",
            "tags": ["transformers", "BERT", "GPT", "natural language processing"],
            "year": 2023,
            "citation_count": 87
        },
        {
            "title": "Distributed Systems Architecture Patterns",
            "content": "Modern distributed systems require careful consideration of consistency, availability, and partition tolerance. This work examines microservices, event-driven architectures, and cloud-native design patterns.",
            "author": "John Smith",
            "category": "Systems",
            "tags": ["distributed systems", "microservices", "cloud computing"],
            "year": 2021,
            "citation_count": 34
        },
        {
            "title": "Database Indexing Strategies for Performance",
            "content": "Effective indexing is crucial for database performance. We compare B-tree indexes, hash indexes, and bitmap indexes across different query patterns and data distributions.",
            "author": "Maria González",
            "category": "Databases", 
            "tags": ["databases", "indexing", "performance", "B-tree"],
            "year": 2022,
            "citation_count": 28
        },
        {
            "title": "Reinforcement Learning in Robotics",
            "content": "Reinforcement learning algorithms enable robots to learn complex behaviors through interaction with their environment. We demonstrate applications in robotic manipulation and autonomous navigation.",
            "author": "Dr. Alex Kim",
            "category": "Robotics",
            "tags": ["reinforcement learning", "robotics", "autonomous systems"],
            "year": 2023,
            "citation_count": 52
        },
        {
            "title": "Quantum Computing Algorithms", 
            "content": "Quantum algorithms like Shor's algorithm and Grover's algorithm demonstrate the potential for exponential speedups over classical computation for specific problem classes.",
            "author": "Prof. Lisa Wang",
            "category": "Quantum Computing",
            "tags": ["quantum computing", "quantum algorithms", "Shor", "Grover"],
            "year": 2021,
            "citation_count": 71
        },
        {
            "title": "Blockchain Consensus Mechanisms",
            "content": "Comparing proof-of-work, proof-of-stake, and delegated proof-of-stake consensus mechanisms in blockchain networks. Analysis of security, scalability, and energy efficiency trade-offs.",
            "author": "Robert Taylor",
            "category": "Blockchain",
            "tags": ["blockchain", "consensus", "proof-of-work", "proof-of-stake"],
            "year": 2022,
            "citation_count": 19
        }
    ]
    
    # Add embeddings to each document
    for doc in documents:
        # Generate embedding from title + content
        text_for_embedding = f"{doc['title']} {doc['content']}"
        doc["content_embedding"] = get_embedding(text_for_embedding)
    
    db.insert_many(documents)
    print(f"Loaded {len(documents)} documents into the search index")


def semantic_search(db: Collection, query: str, limit: int = 5) -> None:
    """Perform semantic search using vector similarity."""
    print(f"\n🔍 SEMANTIC SEARCH: '{query}'")
    print("-" * 60)
    
    # Generate embedding for the query
    query_embedding = get_embedding(query)
    
    # Find semantically similar documents
    results = db.find({}).vector_search("content_embedding", query_embedding, limit=limit).to_list()
    
    if not results:
        print("No results found.")
        return
    
    for i, (doc, similarity) in enumerate(results, 1):
        print(f"{i}. {doc['title']}")
        print(f"   Author: {doc['author']} | Category: {doc['category']} | Citations: {doc['citation_count']}")
        print(f"   Similarity: {similarity:.3f}")
        print(f"   Abstract: {doc['content'][:100]}...")
        print()


def keyword_search(db: Collection, query: str, limit: int = 5) -> None:
    """Perform keyword search using BM25 text search."""
    print(f"\n🔎 KEYWORD SEARCH: '{query}'")
    print("-" * 60)
    
    # Search in both title and content
    title_results = db.find({}).text_search("title", query, limit=limit).to_list()
    content_results = db.find({}).text_search("content", query, limit=limit).to_list()
    
    # Combine and deduplicate results
    all_results = {}
    for doc, score in title_results:
        doc_id = doc["_id"]
        all_results[doc_id] = (doc, score * 1.5)  # Boost title matches
    
    for doc, score in content_results:
        doc_id = doc["_id"]
        if doc_id in all_results:
            # Keep higher score
            if score > all_results[doc_id][1]:
                all_results[doc_id] = (doc, score)
        else:
            all_results[doc_id] = (doc, score)
    
    # Sort by relevance score
    sorted_results = sorted(all_results.values(), key=lambda x: x[1], reverse=True)
    
    if not sorted_results:
        print("No results found.")
        return
    
    for i, (doc, relevance) in enumerate(sorted_results[:limit], 1):
        print(f"{i}. {doc['title']}")
        print(f"   Author: {doc['author']} | Category: {doc['category']} | Citations: {doc['citation_count']}")
        print(f"   Relevance: {relevance:.3f}")
        print(f"   Abstract: {doc['content'][:100]}...")
        print()


def hybrid_search(db: Collection, query: str, limit: int = 5) -> None:
    """Combine semantic and keyword search for better results."""
    print(f"\n🔍🔎 HYBRID SEARCH: '{query}'")
    print("-" * 60)
    
    # Get semantic results
    query_embedding = get_embedding(query)
    semantic_results = db.find({}).vector_search("content_embedding", query_embedding, limit=10).to_list()
    
    # Get keyword results
    title_results = db.find({}).text_search("title", query, limit=10).to_list()
    content_results = db.find({}).text_search("content", query, limit=10).to_list()
    
    # Combine all results with different weights
    combined_scores = {}
    
    # Add semantic similarity scores (weight: 0.4)
    for doc, similarity in semantic_results:
        doc_id = doc["_id"]
        combined_scores[doc_id] = combined_scores.get(doc_id, 0) + (similarity * 0.4)
    
    # Add text relevance scores (weight: 0.6 for content, 0.8 for title)
    for doc, relevance in content_results:
        doc_id = doc["_id"]
        # Normalize BM25 scores to 0-1 range (rough approximation)
        normalized_score = max(0, min(1, (relevance + 5) / 10))
        combined_scores[doc_id] = combined_scores.get(doc_id, 0) + (normalized_score * 0.6)
    
    for doc, relevance in title_results:
        doc_id = doc["_id"]
        normalized_score = max(0, min(1, (relevance + 5) / 10))
        combined_scores[doc_id] = combined_scores.get(doc_id, 0) + (normalized_score * 0.8)
    
    # Get document objects and sort by combined score
    all_docs = {doc["_id"]: doc for doc, _ in semantic_results + title_results + content_results}
    
    ranked_results = [
        (all_docs[doc_id], score) 
        for doc_id, score in sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)
    ]
    
    if not ranked_results:
        print("No results found.")
        return
    
    for i, (doc, combined_score) in enumerate(ranked_results[:limit], 1):
        print(f"{i}. {doc['title']}")
        print(f"   Author: {doc['author']} | Category: {doc['category']} | Citations: {doc['citation_count']}")
        print(f"   Combined Score: {combined_score:.3f}")
        print(f"   Abstract: {doc['content'][:100]}...")
        print()


def filtered_search(db: Collection, category: str, min_citations: int, query: str) -> None:
    """Demonstrate search with pre-filtering."""
    print(f"\n🔍📂 FILTERED SEARCH")
    print(f"Category: {category} | Min Citations: {min_citations} | Query: '{query}'")
    print("-" * 60)
    
    # Apply filters first, then search
    query_embedding = get_embedding(query)
    results = (
        db.find({
            "category": category,
            "citation_count": {"$gte": min_citations}
        })
        .vector_search("content_embedding", query_embedding, limit=5)
        .to_list()
    )
    
    if not results:
        print("No results found with the specified filters.")
        return
    
    for i, (doc, similarity) in enumerate(results, 1):
        print(f"{i}. {doc['title']}")
        print(f"   Author: {doc['author']} | Citations: {doc['citation_count']}")
        print(f"   Similarity: {similarity:.3f}")
        print(f"   Tags: {', '.join(doc['tags'])}")
        print()


def search_by_author_and_topic(db: Collection, author_partial: str, topic_query: str) -> None:
    """Search for papers by author name with topic relevance."""
    print(f"\n👤🔍 AUTHOR + TOPIC SEARCH")
    print(f"Author contains: '{author_partial}' | Topic: '{topic_query}'")
    print("-" * 60)
    
    # Find documents by author using text search on author field
    author_results = db.find({}).text_search("author", author_partial, limit=10).to_list()
    
    if not author_results:
        print(f"No papers found by authors matching '{author_partial}'")
        return
    
    # From those results, find the most topically relevant using vector search
    author_doc_ids = {doc["_id"] for doc, _ in author_results}
    query_embedding = get_embedding(topic_query)
    
    # Filter to only documents from matching authors, then rank by topic relevance
    topic_results = db.find({}).vector_search("content_embedding", query_embedding, limit=20).to_list()
    
    # Keep only results that match both criteria
    combined_results = [
        (doc, similarity) for doc, similarity in topic_results 
        if doc["_id"] in author_doc_ids
    ]
    
    if not combined_results:
        print(f"No papers by '{author_partial}' found matching topic '{topic_query}'")
        return
    
    for i, (doc, similarity) in enumerate(combined_results[:5], 1):
        print(f"{i}. {doc['title']}")
        print(f"   Author: {doc['author']} | Year: {doc['year']}")
        print(f"   Topic Relevance: {similarity:.3f}")
        print(f"   Abstract: {doc['content'][:100]}...")
        print()


def main():
    """Demonstrate the document search system."""
    print("=== MooFile Document Search Engine Demo ===")
    print("Showcasing Vector Similarity + BM25 Text Search")
    print()
    
    # Create temporary database
    db_path = os.path.join(tempfile.mkdtemp(), "document_search.bson")
    
    with Collection(
        db_path,
        indexes=["category", "author", "year"],
        vector_indexes={"content_embedding": 384},  
        text_indexes=["title", "content", "author"]
    ) as db:
        
        # Load sample documents
        seed_documents(db)
        
        # Example 1: Semantic search - finds conceptually similar documents
        semantic_search(db, "artificial intelligence learning algorithms", limit=4)
        
        # Example 2: Keyword search - finds documents with specific terms
        keyword_search(db, "neural networks computer vision", limit=4)
        
        # Example 3: Hybrid search - combines both approaches  
        hybrid_search(db, "deep learning transformers", limit=4)
        
        # Example 4: Filtered search - search within specific categories
        filtered_search(db, "AI/ML", 40, "machine learning algorithms", )
        
        # Example 5: Complex query - author + topic relevance
        search_by_author_and_topic(db, "Chen", "machine learning")
        
        # Show some statistics
        print(f"\n📊 DATABASE STATISTICS")
        print("-" * 60)
        stats = db.stats()
        print(f"Total documents: {stats['documents']}")
        print(f"Database size: {stats['file_size_bytes'] / 1024:.1f} KB")
        print(f"Dead records ratio: {stats['dead_ratio']:.1%}")
        
        print(f"\n✅ Demo complete! Database saved to: {db_path}")
        print("\nFeatures demonstrated:")
        print("• Vector similarity search for semantic matching")
        print("• BM25 text search for keyword matching") 
        print("• Hybrid search combining both approaches")
        print("• Pre-filtering by metadata before search")
        print("• Complex multi-field search scenarios")


if __name__ == "__main__":
    main()