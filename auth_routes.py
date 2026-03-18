"""
auth.py — User authentication for Slaves Panel
SQLite + hashlib (no extra deps)
Session via signed cookies (itsdangerous)
"""
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from datetime import datetime
from typing import Optional

from fastapi import Request
from fastapi.responses import RedirectResponse

# ── Config ────────────────────────────────────────
_BASE    = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(_BASE, "users.db")
SECRET   = os.environ.get("PANEL_SECRET", "change_me_in_production_12345")
CAPTCHA_SECRET = os.environ.get("RECAPTCHA_SECRET", "6LcIwY0sAAAAAPH7R7WAqQfe98daCqWXsocWfsRd")
CAPTCHA_SITE   = os.environ.get("RECAPTCHA_SITE",   "6LcIwY0sAAAAALnE-RjBoQ7BhhE2hg01zuqal7Rj")

SESSION_TTL = 60 * 60 * 24 * 7  # 7 days

# ── DB setup ──────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                username   TEXT    UNIQUE NOT NULL,
                email      TEXT    UNIQUE,
                pw_hash    TEXT    NOT NULL,
                role       TEXT    NOT NULL DEFAULT 'user',
                created_at TEXT    NOT NULL,
                last_login TEXT,
                is_active  INTEGER NOT NULL DEFAULT 1,
                x_init_data TEXT   DEFAULT '',
                vk_user_id  INTEGER DEFAULT NULL,
                vk_first    TEXT   DEFAULT '',
                vk_last     TEXT   DEFAULT ''
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                created_at REAL    NOT NULL,
                expires_at REAL    NOT NULL
            )
        """)
        db.commit()
    # Create default admin if no users exist
    with get_db() as db:
        count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if count == 0:
            create_user("admin", "admin@localhost", "admin123", role="admin")
            print("[auth] Default admin created — login: admin / password: admin123")

# ── Password ──────────────────────────────────────
def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return f"{salt}:{h.hex()}"

def verify_password(password: str, pw_hash: str) -> bool:
    try:
        salt, stored = pw_hash.split(":", 1)
        h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
        return hmac.compare_digest(h.hex(), stored)
    except Exception:
        return False

# ── User CRUD ─────────────────────────────────────
def create_user(username: str, email: str, password: str, role: str = "user") -> Optional[dict]:
    try:
        with get_db() as db:
            db.execute(
                "INSERT INTO users (username, email, pw_hash, role, created_at) VALUES (?,?,?,?,?)",
                (username.strip(), email.strip() if email else None,
                 hash_password(password), role, datetime.now().isoformat())
            )
            db.commit()
        return get_user_by_username(username)
    except sqlite3.IntegrityError:
        return None

def get_user_by_id(user_id: int) -> Optional[dict]:
    with get_db() as db:
        row = db.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return dict(row) if row else None

def get_user_by_username(username: str) -> Optional[dict]:
    with get_db() as db:
        row = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        return dict(row) if row else None

def get_all_users() -> list:
    with get_db() as db:
        rows = db.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]

def update_user(user_id: int, **kwargs):
    allowed = {"email","role","is_active","x_init_data","vk_user_id","vk_first","vk_last","last_login"}
    fields  = {k:v for k,v in kwargs.items() if k in allowed}
    if not fields: return
    sets = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [user_id]
    with get_db() as db:
        db.execute(f"UPDATE users SET {sets} WHERE id=?", vals)
        db.commit()

def change_password(user_id: int, new_password: str):
    with get_db() as db:
        db.execute("UPDATE users SET pw_hash=? WHERE id=?", (hash_password(new_password), user_id))
        db.commit()

def delete_user(user_id: int):
    with get_db() as db:
        db.execute("DELETE FROM users WHERE id=?", (user_id,))
        db.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        db.commit()

# ── Sessions ──────────────────────────────────────
def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    now   = time.time()
    with get_db() as db:
        db.execute(
            "INSERT INTO sessions (token,user_id,created_at,expires_at) VALUES (?,?,?,?)",
            (token, user_id, now, now + SESSION_TTL)
        )
        db.commit()
    return token

def get_session_user(token: str) -> Optional[dict]:
    if not token: return None
    with get_db() as db:
        row = db.execute(
            "SELECT s.user_id, s.expires_at FROM sessions s WHERE s.token=?", (token,)
        ).fetchone()
        if not row: return None
        if time.time() > row["expires_at"]:
            db.execute("DELETE FROM sessions WHERE token=?", (token,))
            db.commit()
            return None
        return get_user_by_id(row["user_id"])

def delete_session(token: str):
    with get_db() as db:
        db.execute("DELETE FROM sessions WHERE token=?", (token,))
        db.commit()

def cleanup_sessions():
    with get_db() as db:
        db.execute("DELETE FROM sessions WHERE expires_at < ?", (time.time(),))
        db.commit()

# ── Cookie helpers ────────────────────────────────
COOKIE_NAME = "slv_session"

def get_current_user(request: Request) -> Optional[dict]:
    token = request.cookies.get(COOKIE_NAME)
    return get_session_user(token) if token else None

def require_user(request: Request):
    """Returns user dict or RedirectResponse to /login"""
    user = get_current_user(request)
    if not user or not user.get("is_active"):
        return RedirectResponse("/login", status_code=302)
    return user

def require_admin(request: Request):
    """Returns user dict or RedirectResponse"""
    user = get_current_user(request)
    if not user or not user.get("is_active"):
        return RedirectResponse("/login", status_code=302)
    if user.get("role") != "admin":
        return RedirectResponse("/dashboard", status_code=302)
    return user

# ── reCAPTCHA v3 ──────────────────────────────────
import urllib.request
import urllib.parse

async def verify_captcha(token: str, min_score: float = 0.5) -> bool:
    """Verify reCAPTCHA v3 token. Returns True if valid."""
    if not CAPTCHA_SECRET:
        return True  # Skip if not configured
    if not token:
        return False
    try:
        data = urllib.parse.urlencode({
            "secret":   CAPTCHA_SECRET,
            "response": token,
        }).encode()
        req = urllib.request.Request(
            "https://www.google.com/recaptcha/api/siteverify",
            data=data, method="POST"
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            result = json.loads(resp.read().decode())
        return result.get("success") and result.get("score", 0) >= min_score
    except Exception as e:
        print(f"[captcha] error: {e}")
        return True  # Fail open to not block users if reCAPTCHA is down