"""
Contacts app example.

A realistic use-case: a small CLI-style contacts manager backed by MooFile.
Demonstrates filtering, sorting, method chains, update operators,
vector similarity search, and text search capabilities.
"""

import os
import tempfile
import random
import numpy as np
from moofile import Collection

DB_PATH = os.path.join(tempfile.mkdtemp(), "contacts.bson")
INDEXES = ["email", "company", "tag"]
VECTOR_INDEXES = {"interests_vector": 8}  # 8-dimensional interest vectors
TEXT_INDEXES = ["bio", "notes"]


def seed(db: Collection) -> None:
    """Populate the database with sample contacts."""
    
    # Interest dimensions: [tech, finance, marketing, sales, design, data_science, AI, leadership]
    contacts = [
        {
            "name": "Alice Zhao", "email": "alice@acme.com", "company": "Acme", "age": 32, 
            "tags": ["vip", "customer"], "score": 92,
            "bio": "Senior software engineer specializing in machine learning and data analytics",
            "notes": "Expert in Python, TensorFlow, and cloud architecture. Very interested in AI applications.",
            "interests_vector": [0.9, 0.2, 0.1, 0.3, 0.2, 0.8, 0.9, 0.4]  # High tech, data science, AI
        },
        {
            "name": "Bob Müller", "email": "bob@widgets.com", "company": "Widgets", "age": 45,
            "tags": ["customer"], "score": 78,
            "bio": "CFO with extensive experience in financial planning and enterprise software",
            "notes": "Looking for data analytics solutions to improve financial forecasting",
            "interests_vector": [0.3, 0.9, 0.2, 0.4, 0.1, 0.6, 0.3, 0.8]  # High finance, leadership
        },
        {
            "name": "Carol Smith", "email": "carol@acme.com", "company": "Acme", "age": 28,
            "tags": ["lead"], "score": 55,
            "bio": "Marketing coordinator passionate about digital campaigns and user experience",
            "notes": "Interested in machine learning for customer segmentation and personalization",
            "interests_vector": [0.4, 0.3, 0.9, 0.6, 0.7, 0.5, 0.6, 0.3]  # High marketing, design
        },
        {
            "name": "Dave Park", "email": "dave@globex.com", "company": "Globex", "age": 38,
            "tags": ["vip", "partner"], "score": 88,
            "bio": "VP of Engineering leading digital transformation initiatives",
            "notes": "Implementing AI-driven automation across the organization. Strong technical background.",
            "interests_vector": [0.8, 0.4, 0.3, 0.2, 0.3, 0.7, 0.8, 0.9]  # High tech, AI, leadership
        },
        {
            "name": "Eve Torres", "email": "eve@globex.com", "company": "Globex", "age": 29,
            "tags": ["customer"], "score": 61,
            "bio": "Data scientist focused on predictive analytics and business intelligence",
            "notes": "PhD in statistics, building machine learning models for customer analytics",
            "interests_vector": [0.7, 0.5, 0.4, 0.3, 0.2, 0.9, 0.8, 0.4]  # High data science, AI
        },
        {
            "name": "Frank Li", "email": "frank@widgets.com", "company": "Widgets", "age": 52,
            "tags": ["partner"], "score": 74,
            "bio": "Sales director with expertise in B2B software and enterprise solutions",
            "notes": "Looking for AI tools to enhance sales processes and customer relationship management",
            "interests_vector": [0.4, 0.6, 0.5, 0.9, 0.3, 0.4, 0.5, 0.7]  # High sales, leadership
        },
        {
            "name": "Grace Kim", "email": "grace@acme.com", "company": "Acme", "age": 35,
            "tags": ["vip"], "score": 95,
            "bio": "Product design lead specializing in AI-powered user interfaces",
            "notes": "Expert in UX/UI design for machine learning applications. Highly influential in design community.",
            "interests_vector": [0.6, 0.3, 0.6, 0.4, 0.9, 0.5, 0.7, 0.6]  # High design, some AI
        },
        {
            "name": "Hank Brown", "email": "hank@startup.io", "company": "Startup", "age": 24,
            "tags": ["lead"], "score": 40,
            "bio": "Junior developer learning about data science and web development",
            "notes": "Recent computer science graduate interested in machine learning career path",
            "interests_vector": [0.8, 0.2, 0.3, 0.2, 0.4, 0.6, 0.5, 0.2]  # High tech, some data science
        },
    ]
    
    db.insert_many(contacts)


def list_by_company(db: Collection, company: str) -> None:
    print(f"\nContacts at {company}:")
    contacts = (
        db.find({"company": company})
        .sort("name")
        .to_list()
    )
    for c in contacts:
        print(f"  {c['name']:<20} {c['email']:<30} score={c['score']}")


def top_vip_contacts(db: Collection, limit: int = 3) -> None:
    print(f"\nTop {limit} VIP contacts by score:")
    # $elemMatch to find docs where the 'tags' array contains 'vip'
    results = (
        db.find({"tags": {"$elemMatch": {"$eq": "vip"}}})
        .sort("score", descending=True)
        .limit(limit)
        .to_list()
    )
    for r in results:
        print(f"  {r['name']:<20} score={r['score']}")


def contacts_needing_follow_up(db: Collection) -> None:
    print("\nLeads and low-score customers to follow up:")
    results = db.find({
        "$or": [
            {"tags": {"$elemMatch": {"$eq": "lead"}}},
            {"score": {"$lt": 65}},
        ]
    }).sort("score").to_list()
    for r in results:
        print(f"  {r['name']:<20} score={r['score']}")


def update_score(db: Collection, email: str, delta: int) -> None:
    db.update_one({"email": email}, inc={"score": delta})
    new_score = db.find_one({"email": email})["score"]
    print(f"\nUpdated {email}: score now {new_score}")


def remove_company(db: Collection, company: str) -> None:
    n = db.delete_many({"company": company})
    print(f"\nRemoved {n} contacts from {company}")


def find_similar_contacts(db: Collection, contact_email: str, limit: int = 3) -> None:
    """Find contacts with similar interests using vector similarity."""
    print(f"\nFinding contacts with similar interests to {contact_email}:")
    
    # Get the reference contact
    reference = db.find_one({"email": contact_email})
    if not reference:
        print(f"  Contact {contact_email} not found")
        return
    
    query_vector = reference["interests_vector"]
    
    # Find similar contacts (excluding the reference contact)
    results = (
        db.find({"email": {"$ne": contact_email}})
        .vector_search("interests_vector", query_vector, limit=limit)
        .to_list()
    )
    
    print(f"  Most similar to {reference['name']}:")
    for contact, similarity in results:
        print(f"    {contact['name']:<20} (similarity: {similarity:.3f}) - {contact['bio'][:50]}...")


def search_by_expertise(db: Collection, query_terms: str, limit: int = 3) -> None:
    """Search contacts by expertise using text search on bio and notes."""
    print(f"\nSearching for contacts with expertise in: '{query_terms}'")
    
    # Search in bio field
    bio_results = db.find({}).text_search("bio", query_terms, limit=limit).to_list()
    
    # Search in notes field  
    notes_results = db.find({}).text_search("notes", query_terms, limit=limit).to_list()
    
    # Combine and deduplicate results
    all_results = {}
    for contact, score in bio_results:
        email = contact["email"]
        all_results[email] = (contact, score, "bio")
    
    for contact, score in notes_results:
        email = contact["email"]
        if email in all_results:
            # Keep higher score
            if score > all_results[email][1]:
                all_results[email] = (contact, score, "notes")
        else:
            all_results[email] = (contact, score, "notes")
    
    # Sort by score and display
    sorted_results = sorted(all_results.values(), key=lambda x: x[1], reverse=True)
    
    print("  Results ranked by relevance:")
    for contact, score, source in sorted_results[:limit]:
        print(f"    {contact['name']:<20} (score: {score:.3f}, from {source}) - {contact['company']}")


def find_ai_experts(db: Collection) -> None:
    """Demonstrate combined search: find AI experts among VIP/partner contacts."""
    print("\nFinding AI experts among VIP/partner contacts (combined search):")
    
    # Create a profile for someone very interested in AI
    ai_query_vector = [0.6, 0.3, 0.3, 0.3, 0.4, 0.8, 0.9, 0.5]  # High AI and data science
    
    # Search among VIP and partner contacts only
    results = (
        db.find({"tags": {"$in": ["vip", "partner"]}})
        .vector_search("interests_vector", ai_query_vector, limit=5)
        .to_list()
    )
    
    print("  VIP/Partner contacts most aligned with AI interests:")
    for contact, similarity in results:
        print(f"    {contact['name']:<20} (similarity: {similarity:.3f}) - {', '.join(contact['tags'])}")
        
    # Also search for "AI" or "machine learning" expertise in their text
    text_results = (
        db.find({"tags": {"$in": ["vip", "partner"]}})
        .text_search("bio", "artificial intelligence machine learning AI", limit=3)
        .to_list()
    )
    
    print("\n  VIP/Partner contacts mentioning AI/ML in bio:")
    for contact, relevance in text_results:
        print(f"    {contact['name']:<20} (relevance: {relevance:.3f}) - {contact['bio'][:60]}...")


def main() -> None:
    print("=== MooFile — Contacts App (with Vector & Text Search) ===")

    with Collection(DB_PATH, 
                   indexes=INDEXES, 
                   vector_indexes=VECTOR_INDEXES,
                   text_indexes=TEXT_INDEXES) as db:
        seed(db)
        print(f"Loaded {db.count()} contacts")

        # Traditional queries
        list_by_company(db, "Acme")
        list_by_company(db, "Globex")
        top_vip_contacts(db, limit=3)
        contacts_needing_follow_up(db)

        # === NEW: Vector Search Examples ===
        print("\n" + "="*60)
        print("VECTOR SIMILARITY SEARCH EXAMPLES")
        print("="*60)
        
        # Find contacts similar to Alice (high tech/AI interests)
        find_similar_contacts(db, "alice@acme.com", limit=3)
        
        # Find contacts similar to Bob (high finance interests)  
        find_similar_contacts(db, "bob@widgets.com", limit=3)

        # === NEW: Text Search Examples ===
        print("\n" + "="*60)
        print("TEXT SEARCH EXAMPLES")  
        print("="*60)
        
        # Search for machine learning expertise
        search_by_expertise(db, "machine learning", limit=3)
        
        # Search for design expertise
        search_by_expertise(db, "design user experience", limit=3)
        
        # Search for financial expertise
        search_by_expertise(db, "financial analytics", limit=3)

        # === NEW: Combined Search Examples ===
        print("\n" + "="*60)
        print("COMBINED SEARCH EXAMPLES")
        print("="*60)
        
        # Find AI experts among high-value contacts
        find_ai_experts(db)

        # === Traditional Updates ===
        print("\n" + "="*60)
        print("TRADITIONAL CRM OPERATIONS")
        print("="*60)

        # Boost Grace's score after a great meeting
        update_score(db, "grace@acme.com", 5)

        # Promote a lead who signed up
        db.update_one(
            {"email": "carol@acme.com"},
            set={"tags": ["customer"]},
            inc={"score": 15},
        )
        print("\nCarol converted from lead to customer:")
        carol = db.find_one({"email": "carol@acme.com"})
        print(f"  tags={carol['tags']}, score={carol['score']}")

        # A startup acquisition — remove those contacts
        remove_company(db, "Startup")

        # Stats
        s = db.stats()
        print(f"\nFinal stats: {s['documents']} documents, "
              f"dead_ratio={s['dead_ratio']:.1%}")
        
        print("\n" + "="*60)
        print(f"MooFile v0.2.0 demo complete! Database saved to: {DB_PATH}")
        print("Features demonstrated:")
        print("- Traditional field indexes and queries")  
        print("- Vector similarity search for interest matching")
        print("- BM25 text search for expertise discovery")
        print("- Combined search with pre-filtering")
        print("="*60)


if __name__ == "__main__":
    main()
