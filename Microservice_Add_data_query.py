import argparse
import json
import secrets
import sqlite3
import string
from pathlib import Path

DB_FILE = "codes.db"
TABLE_NAME = "code_entries"


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            code TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            maxviews INTEGER NOT NULL DEFAULT 20,
            views INTEGER NOT NULL DEFAULT 0,
            last_viewed_at TEXT NOT NULL DEFAULT 0
        )
        """
    )

    conn.commit()
    conn.close()


def generate_code(length: int = 6):
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def generate_unique_code(cur, length: int = 6):
    while True:
        code = generate_code(length=length)
        exists = cur.execute(
            f"SELECT 1 FROM {TABLE_NAME} WHERE code = ?",
            (code,),
        ).fetchone()
        if not exists:
            return code


def load_json_entries(json_file: Path):
    text = json_file.read_text(encoding="utf-8").strip()
    if not text:
        return []

    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        raise ValueError("Top-level JSON must be an object or an array of objects")
    except json.JSONDecodeError:
        # Fallback: JSON Lines format (one JSON object per line).
        entries = []
        for lineno, raw in enumerate(text.splitlines(), start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {lineno}: {exc}") from exc
            entries.append(parsed)
        return entries


def import_json(json_file: Path):
    if not json_file.exists():
        raise FileNotFoundError(f"File not found: {json_file}")

    inserted = 0
    skipped = 0

    conn = get_db_connection()
    cur = conn.cursor()
    entries = load_json_entries(json_file)

    for idx, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            print(f"[entry {idx}] skipped: entry is not a JSON object")
            skipped += 1
            continue

        payload = json.dumps(entry, ensure_ascii=False)
        code = generate_unique_code(cur, length=6)
        cur.execute(
            f"""
            INSERT INTO {TABLE_NAME} (code, payload, maxviews, views)
            VALUES (?, ?, 20, 0)
            """,
            (code, payload),
        )
        inserted += 1
        print(f"[entry {idx}] inserted: {code}")

    conn.commit()
    conn.close()

    return {"inserted": inserted, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(
        description="Import JSON data into SQLite with generated 6-char codes."
    )
    parser.add_argument(
        "--json-file",
        default="cv.json",
        help="Path to JSON file (object, array of objects, or JSONL).",
    )
    args = parser.parse_args()

    init_db()
    result = import_json(Path(args.json_file))
    print(
        f"\nImport summary: inserted={result['inserted']}, skipped={result['skipped']}"
    )


if __name__ == "__main__":
    main()
