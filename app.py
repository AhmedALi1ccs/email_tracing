import os
import base64
import psycopg2
from urllib.parse import unquote, quote
from flask import Flask, request, make_response, redirect

PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

app = Flask(__name__)

def _client_ip():
    # Prefer real client IP if behind a proxy/CDN
    return (
        request.headers.get("CF-Connecting-IP")
        or (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        or request.remote_addr
        or ""
    )

def _is_suspect_open(ua: str, ip: str, via: str) -> bool:
    u = (ua or "").lower()
    v = (via or "").lower()
    # Gmail image cache
    if "googleimageproxy" in u or "gmailimageproxy" in u:
        return True
    # Apple Mail Privacy Protection / Apple image proxies
    if "appleimageproxy" in u or ip.startswith("17."):
        return True
    # Any explicit proxy hint
    if "proxy" in v:
        return True
    return False

def record_open(tracking_id: int, email: str, ua: str, ip: str, via: str, suspect: bool):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            # Upsert the open. Keep it non-suspect once confirmed.
            cur.execute(
                """
                INSERT INTO email_opens (tracking_id, email, open_time, user_agent, ip, via, is_suspect)
                VALUES (%s, %s, NOW(), %s, %s, %s, %s)
                ON CONFLICT (tracking_id, email)
                DO UPDATE SET
                  open_time = GREATEST(email_opens.open_time, EXCLUDED.open_time),
                  user_agent = EXCLUDED.user_agent,
                  ip = EXCLUDED.ip,
                  via = EXCLUDED.via,
                  is_suspect = email_opens.is_suspect AND EXCLUDED.is_suspect
                """,
                (tracking_id, email, ua, ip, via, suspect),
            )
        conn.commit()
    except Exception:
        pass
    finally:
        if conn:
            conn.close()

@app.route("/open.gif")
def open_gif():
    # Example: /open.gif?campaign=123&email=a%40b.com&ts=...
    campaign = request.args.get("campaign")
    email = request.args.get("email")
    ua = request.headers.get("User-Agent", "")
    via = request.headers.get("Via", "")
    ip = _client_ip()
    if campaign and email:
        try:
            email_norm = unquote(email).strip().lower()
            suspect = _is_suspect_open(ua, ip, via)
            record_open(int(campaign), email_norm, ua, ip, via, suspect)
        except Exception:
            pass
    resp = make_response(PIXEL_GIF)
    resp.headers["Content-Type"] = "image/gif"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.route("/c")
def click_redirect():
    """
    Click redirect: /c?campaign=123&email=a%40b.com&u=<urlencoded target>
    - records the click
    - flips any suspect open to confirmed (is_suspect = FALSE)
    - ensures an open row exists even if pixel was blocked
    """
    campaign = request.args.get("campaign")
    email = (request.args.get("email") or "").strip().lower()
    url = request.args.get("u") or "/"
    ua = request.headers.get("User-Agent", "")
    via = request.headers.get("Via", "")
    ip = _client_ip()
    try:
        if campaign and email:
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    # record click
                    cur.execute(
                        """
                        INSERT INTO email_clicks (tracking_id, email, url, click_time)
                        VALUES (%s, %s, %s, NOW())
                        """,
                        (int(campaign), email, unquote(url)),
                    )
                    # ensure there's an open row and mark it confirmed
                    cur.execute(
                        """
                        INSERT INTO email_opens (tracking_id, email, open_time, user_agent, ip, via, is_suspect)
                        VALUES (%s, %s, NOW(), %s, %s, %s, FALSE)
                        ON CONFLICT (tracking_id, email)
                        DO UPDATE SET
                          is_suspect = FALSE,
                          user_agent = EXCLUDED.user_agent,
                          ip = EXCLUDED.ip,
                          via = EXCLUDED.via
                        """,
                        (int(campaign), email, ua, ip, via),
                    )
                conn.commit()
            finally:
                conn.close()
    except Exception:
        pass
    return redirect(unquote(url), code=302)

@app.route("/")
def ok():
    return "OK"
