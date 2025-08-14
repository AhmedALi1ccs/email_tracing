import os, base64, psycopg2
from urllib.parse import unquote, quote
from flask import Flask, request, make_response, redirect

PIXEL_GIF = base64.b64decode("R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

def get_conn():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

app = Flask(__name__)

def _client_ip():
    return (
        request.headers.get("CF-Connecting-IP")
        or (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        or request.remote_addr
        or ""
    )

def _looks_proxy(ua: str, ip: str, via: str) -> bool:
    u = (ua or "").lower()
    v = (via or "").lower()
    if "googleimageproxy" in u or "gmailimageproxy" in u:
        return True
    if "appleimageproxy" in u or ip.startswith("17."):
        return True
    if "proxy" in v:
        return True
    return False

@app.route("/open.gif")
def open_gif():
    campaign = request.args.get("campaign")
    email = request.args.get("email")
    ua = request.headers.get("User-Agent", "")
    via = request.headers.get("Via", "")
    ip = _client_ip()
    if campaign and email:
        try:
            suspect = _looks_proxy(ua, ip, via)
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO email_opens (tracking_id, email, open_time, user_agent, ip, via, is_suspect)
                        VALUES (%s, %s, NOW(), %s, %s, %s, %s)
                    """, (int(campaign), unquote(email).strip().lower(), ua, ip, via, suspect))
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass
    resp = make_response(PIXEL_GIF)
    resp.headers["Content-Type"] = "image/gif"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.route("/c")
def click_redirect():
    # /c?campaign=123&email=a%40b.com&u=<urlencoded target>
    campaign = request.args.get("campaign")
    email = (request.args.get("email") or "").strip().lower()
    url = request.args.get("u") or "/"
    try:
        if campaign and email:
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO email_clicks (tracking_id, email, url, click_time)
                        VALUES (%s, %s, %s, NOW())
                    """, (int(campaign), email, unquote(url)))
                conn.commit()
            finally:
                conn.close()
    except Exception:
        pass
    return redirect(unquote(url), code=302)

@app.route("/")
def ok():
    return "OK"
