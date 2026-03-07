"""
Contacts app example.

A realistic use-case: a small CLI-style contacts manager backed by MooFile.
Demonstrates filtering, sorting, method chains, and update operators.
"""

import os
import tempfile
from moofile import Collection

DB_PATH = os.path.join(tempfile.mkdtemp(), "contacts.bson")
INDEXES = ["email", "company", "tag"]


def seed(db: Collection) -> None:
    """Populate the database with sample contacts."""
    db.insert_many([
        {"name": "Alice Zhao",   "email": "alice@acme.com",     "company": "Acme",   "age": 32, "tags": ["vip", "customer"], "score": 92},
        {"name": "Bob Müller",   "email": "bob@widgets.com",    "company": "Widgets","age": 45, "tags": ["customer"],        "score": 78},
        {"name": "Carol Smith",  "email": "carol@acme.com",     "company": "Acme",   "age": 28, "tags": ["lead"],            "score": 55},
        {"name": "Dave Park",    "email": "dave@globex.com",    "company": "Globex", "age": 38, "tags": ["vip", "partner"],  "score": 88},
        {"name": "Eve Torres",   "email": "eve@globex.com",     "company": "Globex", "age": 29, "tags": ["customer"],        "score": 61},
        {"name": "Frank Li",     "email": "frank@widgets.com",  "company": "Widgets","age": 52, "tags": ["partner"],         "score": 74},
        {"name": "Grace Kim",    "email": "grace@acme.com",     "company": "Acme",   "age": 35, "tags": ["vip"],             "score": 95},
        {"name": "Hank Brown",   "email": "hank@startup.io",    "company": "Startup","age": 24, "tags": ["lead"],            "score": 40},
    ])


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


def main() -> None:
    print("=== MooFile — Contacts App ===")

    with Collection(DB_PATH, indexes=INDEXES) as db:
        seed(db)
        print(f"Loaded {db.count()} contacts")

        list_by_company(db, "Acme")
        list_by_company(db, "Globex")

        top_vip_contacts(db, limit=3)
        contacts_needing_follow_up(db)

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


if __name__ == "__main__":
    main()
