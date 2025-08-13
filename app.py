import os
from flask import Flask, request, make_response
import base64, psycopg2
from urllib.parse import unquote

PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # REQUIRED for hosted DB
DB_HOST = os.getenv("DB_HOST")          # e.g. db-postgres-xyz.render.com
DB_PORT = int(os.getenv("DB_PORT", "5432"))

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

def record_open(tracking_id: int, email: str):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            # idempotent (unique index on (tracking_id,email) must exist)
            cur.execute("""
                INSERT INTO email_opens (tracking_id, email, open_time)
                VALUES (%s, %s, NOW())
                ON CONFLICT (tracking_id, email) DO NOTHING
            """, (tracking_id, email))
            if cur.rowcount == 1:
                cur.execute("""
                    UPDATE email_tracking
                    SET open_count = COALESCE(open_count, 0) + 1
                    WHERE id = %s
                """, (tracking_id,))
        conn.commit()
    except Exception:
        # swallow to keep pixel fast & reliable
        pass
    finally:
        if conn: conn.close()

app = Flask(__name__)

@app.route("/open.gif")
def open_gif():
    # Example URL: https://your-tracker.com/open.gif?campaign=123&email=a%40b.com&ts=123456
    campaign = request.args.get("campaign")
    email = request.args.get("email")
    if campaign and email:
        try:
            record_open(int(campaign), unquote(email).strip().lower())
        except Exception:
            pass
    resp = make_response(PIXEL_GIF)
    resp.headers["Content-Type"] = "image/gif"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.route("/")
def ok():
    return "OK"
