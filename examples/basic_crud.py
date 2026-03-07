"""
Basic CRUD example — the fastest path to understanding MooFile.

Shows insert, find, update, delete, and persistence.
"""

import os
import tempfile
from moofile import Collection, DuplicateKeyError, DocumentNotFoundError

# Use a temporary file so this example is self-contained and cleans up after itself.
tmp = tempfile.mkdtemp()
PATH = os.path.join(tmp, "contacts.bson")


def main():
    print("=== MooFile — Basic CRUD ===\n")

    # -------------------------------------------------------------------------
    # Open (or create) a collection with indexes on two fields
    # -------------------------------------------------------------------------
    with Collection(PATH, indexes=["email", "status"]) as db:

        # ---------------------------------------------------------------------
        # INSERT
        # ---------------------------------------------------------------------
        alice = db.insert({
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30,
            "status": "active",
        })
        print(f"Inserted Alice: _id={alice['_id']}")

        # insert_many for bulk writes
        more = db.insert_many([
            {"name": "Bob",   "email": "bob@example.com",   "age": 25, "status": "trial"},
            {"name": "Carol", "email": "carol@example.com", "age": 40, "status": "active"},
            {"name": "Dave",  "email": "dave@example.com",  "age": 22, "status": "inactive"},
        ])
        print(f"Inserted {len(more)} more contacts\n")

        # _id is always available after insert
        print(f"Total documents: {db.count()}")

        # ---------------------------------------------------------------------
        # FIND
        # ---------------------------------------------------------------------
        print("\n--- Find ---")

        # Exact match (uses the 'status' index)
        active = db.find({"status": "active"}).to_list()
        print(f"Active contacts ({len(active)}): {[d['name'] for d in active]}")

        # Range query (uses the 'age' field — not indexed here, so full scan)
        young = db.find({"age": {"$lt": 30}}).sort("age").to_list()
        print(f"Under 30 ({len(young)}): {[(d['name'], d['age']) for d in young]}")

        # find_one returns a dict or None
        alice_doc = db.find_one({"email": "alice@example.com"})
        print(f"find_one(alice): {alice_doc['name']}, age={alice_doc['age']}")

        # exists is a handy shortcut
        print(f"Does alice exist? {db.exists({'email': 'alice@example.com'})}")
        print(f"Does zara exist?  {db.exists({'email': 'zara@example.com'})}")

        # ---------------------------------------------------------------------
        # UPDATE
        # ---------------------------------------------------------------------
        print("\n--- Update ---")

        # $set — update specific fields
        db.update_one({"email": "alice@example.com"}, set={"age": 31})
        print(f"Alice's age after update: {db.find_one({'email': 'alice@example.com'})['age']}")

        # $inc — increment a field
        db.insert({"name": "Eve", "email": "eve@example.com", "logins": 5, "status": "active"})
        db.update_one({"email": "eve@example.com"}, inc={"logins": 1})
        print(f"Eve's logins after increment: {db.find_one({'email': 'eve@example.com'})['logins']}")

        # $unset — remove a field
        db.update_one({"email": "eve@example.com"}, unset=["logins"])
        print(f"'logins' still exists after unset: {'logins' in db.find_one({'email': 'eve@example.com'})}")

        # update_many
        updated = db.update_many({"status": "trial"}, set={"status": "active"})
        print(f"Promoted {updated} trial user(s) to active")

        # replace_one — swap the entire document (preserves _id)
        db.replace_one({"name": "Dave"}, {"name": "Dave D.", "email": "daved@example.com", "status": "active", "age": 23})
        print(f"Dave after replace: {db.find_one({'email': 'daved@example.com'})}")

        # ---------------------------------------------------------------------
        # DELETE
        # ---------------------------------------------------------------------
        print("\n--- Delete ---")
        db.insert({"name": "Temp", "email": "temp@example.com", "status": "inactive"})
        deleted = db.delete_one({"email": "temp@example.com"})
        print(f"Deleted temp user: {deleted}")

        removed = db.delete_many({"status": "inactive"})
        print(f"Purged {removed} inactive user(s)")
        print(f"Remaining documents: {db.count()}")

        # ---------------------------------------------------------------------
        # ERROR HANDLING
        # ---------------------------------------------------------------------
        print("\n--- Error handling ---")

        # DuplicateKeyError on duplicate _id
        db.insert({"_id": "fixed-id", "name": "Fixed"})
        try:
            db.insert({"_id": "fixed-id", "name": "Duplicate"})
        except DuplicateKeyError as e:
            print(f"Caught DuplicateKeyError: {e}")

        # DocumentNotFoundError when update_one matches nothing
        try:
            db.update_one({"email": "nobody@example.com"}, set={"age": 99})
        except DocumentNotFoundError as e:
            print(f"Caught DocumentNotFoundError: {e}")

        # ---------------------------------------------------------------------
        # STATS and COMPACTION
        # ---------------------------------------------------------------------
        print("\n--- Stats & compaction ---")
        s = db.stats()
        print(f"Documents: {s['documents']}, dead records: {s['dead_records']}, "
              f"dead ratio: {s['dead_ratio']:.1%}")

        if s["dead_ratio"] > 0:
            db.compact()
            s2 = db.stats()
            print(f"After compact — dead records: {s2['dead_records']}")

    # -------------------------------------------------------------------------
    # PERSISTENCE — data survives close/reopen
    # -------------------------------------------------------------------------
    print("\n--- Persistence ---")
    with Collection(PATH, indexes=["email", "status"]) as db:
        print(f"Documents after reopen: {db.count()}")
        print(f"Alice still here: {db.exists({'email': 'alice@example.com'})}")

    print("\nDone.")


if __name__ == "__main__":
    main()
