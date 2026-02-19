from flask import Flask, jsonify
import json
import sqlite3
import os
app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "codes.db")
TABLE_NAME = "code_entries"


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

# for debugging tests if server is running
@app.route("/")
def hello():
    return "Hello, World!"

# Dynamic endpoint: reads value from URL
@app.route("/<string:code>")
def read_code(code):
    conn = get_db_connection()
    row = conn.execute(
        f"""
        SELECT code, payload, maxviews, views, last_viewed_at
        FROM {TABLE_NAME}
        WHERE code = ?
        """,
        (code,),
    ).fetchone()

    if row is None:
        conn.close()
        return jsonify({"error": "Code not found"}), 404

    current_views = row["views"] or 0
    max_views = row["maxviews"] or 20

    if current_views >= max_views:
        conn.close()
        return jsonify(None)

    conn.execute(
        f"""
        UPDATE {TABLE_NAME}
        SET views = COALESCE(views, 0) + 1,
            last_viewed_at
 = CURRENT_TIMESTAMP
        WHERE code = ?
        """,
        (code,),
    )
    conn.commit()
    conn.close()
    return jsonify(
        {
            "code": row["code"],
            "data": json.loads(row["payload"]) if row["payload"] else None,
            "available_views": max_views- (current_views + 1),
        }
    )

if __name__ == "__main__":
    app.run(debug=True)
