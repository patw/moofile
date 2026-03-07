"""
Event log example.

Shows MooFile as an embedded structured event/audit log:
  - append-only inserts (natural fit for the BSON append-only format)
  - querying events by type, severity, and time window
  - compaction to reclaim space after old events are purged
"""

import os
import tempfile
from datetime import datetime, timezone, timedelta
from moofile import Collection, count, collect

DB_PATH = os.path.join(tempfile.mkdtemp(), "events.bson")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts(days_ago: int = 0, hours_ago: int = 0) -> str:
    """Generate an ISO timestamp some time in the past."""
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago, hours=hours_ago)
    return dt.isoformat()


def log_event(db: Collection, event_type: str, severity: str, message: str, **meta) -> dict:
    """Append a structured event to the log."""
    return db.insert({
        "type": event_type,
        "severity": severity,
        "message": message,
        "ts": now_iso(),
        **meta,
    })


def seed(db: Collection) -> None:
    """Insert sample events spanning a few days."""
    events = [
        # Recent events (today)
        {"type": "login",     "severity": "info",    "message": "User login",  "user": "alice", "ts": ts(hours_ago=1)},
        {"type": "login",     "severity": "info",    "message": "User login",  "user": "bob",   "ts": ts(hours_ago=2)},
        {"type": "api_call",  "severity": "info",    "message": "GET /api/v1/data", "user": "alice", "ts": ts(hours_ago=1)},
        {"type": "error",     "severity": "warning", "message": "Rate limit hit", "user": "bob", "ts": ts(hours_ago=2)},

        # Yesterday
        {"type": "login",     "severity": "info",    "message": "User login",  "user": "carol", "ts": ts(days_ago=1)},
        {"type": "error",     "severity": "error",   "message": "DB timeout",  "user": "system","ts": ts(days_ago=1, hours_ago=3)},
        {"type": "api_call",  "severity": "info",    "message": "POST /api/v1/upload", "user": "carol", "ts": ts(days_ago=1)},
        {"type": "audit",     "severity": "info",    "message": "Config changed", "user": "admin", "ts": ts(days_ago=1)},

        # Older
        {"type": "error",     "severity": "critical","message": "Disk full",   "user": "system","ts": ts(days_ago=5)},
        {"type": "audit",     "severity": "info",    "message": "User created","user": "admin", "ts": ts(days_ago=7)},
        {"type": "login",     "severity": "warning", "message": "Failed login","user": "unknown","ts": ts(days_ago=3)},
        {"type": "api_call",  "severity": "info",    "message": "DELETE /api/v1/record/99", "user": "alice", "ts": ts(days_ago=2)},
    ]
    for e in events:
        db.insert(e)


def show_recent_errors(db: Collection) -> None:
    print("\n--- Errors and Warnings (any severity >= warning) ---")
    results = (
        db.find({"severity": {"$in": ["warning", "error", "critical"]}})
        .sort("ts", descending=True)
        .to_list()
    )
    for r in results:
        print(f"  [{r['severity'].upper():<8}] {r['ts'][:19]}  {r['message']}")


def events_by_type(db: Collection) -> None:
    print("\n--- Event count by type ---")
    results = (
        db.find()
        .group("type")
        .agg(count(), collect("severity"))
        .sort("count", descending=True)
        .to_list()
    )
    for r in results:
        severities = set(r["collect_severity"])
        print(f"  {r['type']:<12} {r['count']:>4} events  severities: {sorted(severities)}")


def user_activity(db: Collection, user: str) -> None:
    print(f"\n--- Activity log for '{user}' ---")
    results = (
        db.find({"user": user})
        .sort("ts", descending=True)
        .to_list()
    )
    if not results:
        print(f"  (no events for {user})")
        return
    for r in results:
        print(f"  {r['ts'][:19]}  [{r['type']:<10}] {r['message']}")


def purge_old_events(db: Collection, older_than_days: int = 3) -> None:
    cutoff = ts(days_ago=older_than_days)
    old = db.find({"ts": {"$lt": cutoff}}).to_list()
    print(f"\n--- Purging events older than {older_than_days} days ---")
    print(f"  Found {len(old)} events to purge")
    deleted = db.delete_many({"ts": {"$lt": cutoff}})
    print(f"  Deleted: {deleted}")

    s = db.stats()
    print(f"  dead_ratio after purge: {s['dead_ratio']:.1%}")
    db.compact()
    print(f"  Compacted. Remaining events: {db.count()}")


def main() -> None:
    print("=== MooFile — Event Log ===")

    with Collection(DB_PATH, indexes=["type", "severity", "user"]) as db:
        seed(db)
        print(f"Loaded {db.count()} events")

        show_recent_errors(db)
        events_by_type(db)
        user_activity(db, "alice")
        user_activity(db, "admin")

        # Add a new event live
        log_event(db, "login", "info", "User login", user="dave")
        print(f"\nLogged a new event. Total: {db.count()}")

        purge_old_events(db, older_than_days=3)

        print(f"\nFinal db stats: {db.stats()}")


if __name__ == "__main__":
    main()
