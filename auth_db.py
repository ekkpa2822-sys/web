"""
auth_db.py - SQLite user database + session management
pip install passlib[bcrypt] itsdangerous aiosqlite
"""
import sqlite3
import hashlib
import secrets
import time
import os
from datetime import datetime
from typing import Optional

# ── Config ────────────────────────────────────────────────
# DB saves permanently next to auth_db.py, regardless of working directory
_HERE   = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_HERE, "panel_users.db")
SECRET_KEY   = os.environ.get("PANEL_SECRET", "slv-panel-fixed-secret-key-2025-do-not-change")
SESSION_TTL  = 60 * 60 * 24 * 7   # 7 days
COOKIE_NAME  = "panel_session"

# Google reCAPTCHA keys - set in env or replace here
RECAPTCHA_SITE_KEY   = os.environ.get("RECAPTCHA_SITE",   "6LcIwY0sAAAAALnE-RjBoQ7BhhE2hg01zuqal7Rj")
RECAPTCHA_SECRET_KEY = os.environ.get("RECAPTCHA_SECRET", "6LcIwY0sAAAAAPH7R7WAqQfe98daCqWXsocWfsRd")

# ── DB init ───────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        username    TEXT    UNIQUE NOT NULL,
        email       TEXT    UNIQUE,
        pass_hash   TEXT    NOT NULL,
        role        TEXT    NOT NULL DEFAULT 'user',
        vk_uid      INTEGER,
        vk_first    TEXT    DEFAULT '',
        vk_last     TEXT    DEFAULT '',
        x_init_data TEXT,
        user_token  TEXT,
        created_at  TEXT    NOT NULL,
        last_login  TEXT,
        is_active   INTEGER NOT NULL DEFAULT 1,
        status      TEXT    NOT NULL DEFAULT 'pending_xi',
        xi_token    TEXT    DEFAULT '',
        approved_by INTEGER DEFAULT NULL,
        approved_at TEXT    DEFAULT NULL
    );
    CREATE TABLE IF NOT EXISTS sessions (
        token       TEXT    PRIMARY KEY,
        user_id     INTEGER NOT NULL,
        expires_at  INTEGER NOT NULL,
        ip          TEXT,
        ua          TEXT
    );
    CREATE TABLE IF NOT EXISTS audit_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          TEXT    NOT NULL,
        user_id     INTEGER,
        username    TEXT,
        action      TEXT    NOT NULL,
        target      TEXT,
        detail      TEXT,
        ip          TEXT
    );
    CREATE TABLE IF NOT EXISTS trade_history (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ts          TEXT    NOT NULL,
        user_id     INTEGER NOT NULL,
        username    TEXT    NOT NULL,
        action      TEXT    NOT NULL,
        vk_id       INTEGER,
        vk_name     TEXT,
        price       INTEGER,
        balance_after INTEGER,
        detail      TEXT
    );
    """)
    # Create default admin if no users exist
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        admin_hash = _hash_password("admin123")
        cur.execute("""
            INSERT INTO users (username, email, pass_hash, role, created_at)
            VALUES (?, ?, ?, 'admin', ?)
        """, ("admin", "admin@local", admin_hash, datetime.now().isoformat()))
        print("[auth_db] Default admin created: admin / admin123")
    con.commit()
    # Migrate: add new columns if they don't exist yet
    # Migrate: add new tables if missing
    for tbl in [
        """CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL, user_id INTEGER, username TEXT,
            action TEXT NOT NULL, target TEXT, detail TEXT, ip TEXT)""",
        """CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL, user_id INTEGER NOT NULL, username TEXT NOT NULL,
            action TEXT NOT NULL, vk_id INTEGER, vk_name TEXT,
            price INTEGER, balance_after INTEGER, detail TEXT)""",
    ]:
        try: con.execute(tbl)
        except: pass
    for col_def in [
        "ALTER TABLE users ADD COLUMN vk_first TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN vk_last TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'pending_xi'",
        "ALTER TABLE users ADD COLUMN vk_first TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN vk_last TEXT DEFAULT ''"  ,
        "ALTER TABLE users ADD COLUMN xi_token TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN approved_by INTEGER DEFAULT NULL",
        "ALTER TABLE users ADD COLUMN approved_at TEXT DEFAULT NULL",
    ]:
        try: con.execute(col_def)
        except: pass
    # Existing admin users → mark as active immediately
    con.execute("UPDATE users SET status='active' WHERE role='admin' AND status='pending_xi'")
    con.commit()
    con.close()

def get_pending_users() -> list:
    """Users who submitted x-init-data awaiting admin approval."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM users WHERE status='pending_approval' ORDER BY id").fetchall()
    con.close()
    return [dict(r) for r in rows]

def approve_user(uid: int, admin_id: int) -> bool:
    """Admin approves: activate VK session."""
    user = get_user_by_id(uid)
    if not user or not user.get("xi_token"): return False
    now = datetime.now().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE users SET status='active', approved_by=?, approved_at=?, x_init_data=xi_token WHERE id=?",
                (admin_id, now, uid))
    con.commit()
    con.close()
    return True

def reject_user(uid: int) -> bool:
    """Admin rejects: send back to pending_xi."""
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE users SET status='pending_xi', xi_token='' WHERE id=?", (uid,))
    con.commit()
    con.close()
    return True

# ── Password ──────────────────────────────────────────────
def _hash_password(pw: str) -> str:
    salt = secrets.token_hex(16)
    h    = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 200_000)
    return f"{salt}${h.hex()}"

def check_password(pw: str, stored: str) -> bool:
    try:
        salt, h = stored.split("$", 1)
        return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 200_000).hex() == h
    except Exception:
        return False

# ── User CRUD ─────────────────────────────────────────────
def get_user_by_id(uid: int) -> Optional[dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    con.close()
    return dict(row) if row else None

def get_user_by_username(username: str) -> Optional[dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM users WHERE username=?", (username.strip().lower(),)).fetchone()
    con.close()
    return dict(row) if row else None

def get_all_users() -> list:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM users ORDER BY id").fetchall()
    con.close()
    return [dict(r) for r in rows]

def create_user(username: str, password: str, email: str = None, role: str = "user") -> Optional[dict]:
    uname = username.strip().lower()
    if len(uname) < 3 or len(password) < 6:
        return None
    try:
        status = "active" if role == "admin" else "pending_xi"
        con = sqlite3.connect(DB_PATH)
        con.execute("""
            INSERT INTO users (username, email, pass_hash, role, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (uname, email or None, _hash_password(password), role, datetime.now().isoformat(), status))
        con.commit()
        row = con.execute("SELECT * FROM users WHERE username=?", (uname,)).fetchone()
        con.row_factory = sqlite3.Row
        con.close()
        return get_user_by_username(uname)
    except sqlite3.IntegrityError:
        return None

def update_user(uid: int, **fields):
    allowed = {"email", "role", "vk_uid", "vk_first", "vk_last", "x_init_data", "user_token", "last_login", "is_active", "status", "xi_token", "approved_by", "approved_at"}
    sets, vals = [], []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k}=?")
            vals.append(v)
    if not sets:
        return
    vals.append(uid)
    con = sqlite3.connect(DB_PATH)
    con.execute(f"UPDATE users SET {', '.join(sets)} WHERE id=?", vals)
    con.commit()
    con.close()

def update_password(uid: int, new_password: str):
    if len(new_password) < 6:
        return False
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE users SET pass_hash=? WHERE id=?", (_hash_password(new_password), uid))
    con.commit()
    con.close()
    return True

def delete_user(uid: int):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
    con.execute("DELETE FROM users WHERE id=?", (uid,))
    con.commit()
    con.close()

# ── Sessions ──────────────────────────────────────────────
def create_session(user_id: int, ip: str = None, ua: str = None) -> str:
    token = secrets.token_urlsafe(32)
    exp   = int(time.time()) + SESSION_TTL
    con   = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO sessions (token, user_id, expires_at, ip, ua) VALUES (?,?,?,?,?)",
                (token, user_id, exp, ip, ua))
    con.commit()
    con.close()
    # Update last_login
    update_user(user_id, last_login=datetime.now().isoformat())
    return token

def get_session(token: str) -> Optional[dict]:
    if not token:
        return None
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    row = con.execute(
        "SELECT s.user_id, s.expires_at, s.ip, "
        "u.id, u.username, u.email, u.role, u.is_active, u.status, "
        "u.x_init_data, u.vk_uid, u.vk_first, u.vk_last, u.created_at, u.last_login, "
        "u.xi_token, u.approved_by, u.approved_at "
        "FROM sessions s JOIN users u ON s.user_id=u.id "
        "WHERE s.token=? AND s.expires_at>?",
        (token, int(time.time()))
    ).fetchone()
    con.close()
    if not row:
        return None
    d = dict(row)
    if not d.get("is_active"):
        return None
    return d

def delete_session(token: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM sessions WHERE token=?", (token,))
    con.commit()
    con.close()

def cleanup_sessions():
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM sessions WHERE expires_at<?", (int(time.time()),))
    con.commit()
    con.close()

def count_sessions() -> dict:
    con = sqlite3.connect(DB_PATH)
    now = int(time.time())
    total  = con.execute("SELECT COUNT(*) FROM sessions WHERE expires_at>?", (now,)).fetchone()[0]
    con.close()
    return {"active": total}

# ── reCAPTCHA ─────────────────────────────────────────────
import urllib.request
import json as _json

def verify_recaptcha(token: str, remote_ip: str = None) -> bool:
    """Verify Google reCAPTCHA v3 token."""
    if not RECAPTCHA_SECRET_KEY:
        return True  # капча не настроена - пропускаем
    if not token or len(token.strip()) < 10:
        # токен не пришёл с фронта — пропускаем (invisible recaptcha иногда не успевает)
        return True
    try:
        data = urllib.parse.urlencode({
            "secret":   RECAPTCHA_SECRET_KEY,
            "response": token,
            **({"remoteip": remote_ip} if remote_ip else {}),
        }).encode()
        req = urllib.request.Request(
            "https://www.google.com/recaptcha/api/siteverify",
            data=data, method="POST"
        )
        with urllib.request.urlopen(req, timeout=4) as resp:
            result = _json.loads(resp.read())
        if not result.get("success"):
            print(f"[recaptcha] failed: {result.get('error-codes')}")
            return True  # при ошибке верификации - пропускаем (не блокируем)
        # v3: проверяем score (порог 0.3 - нежёсткий)
        score = result.get("score")
        if score is not None and score < 0.3:
            print(f"[recaptcha] low score: {score}")
            return False
        return True
    except Exception as e:
        print(f"[recaptcha] error: {e}")
        return True  # не блокируем если сервер недоступен


import urllib.parse

# Initialize on import
init_db()

# ── Audit log ─────────────────────────────────────────
def audit(user_id: int, username: str, action: str,
          target: str = None, detail: str = None, ip: str = None):
    """Log any admin/user action to DB."""
    try:
        with get_db() as db:
            db.execute(
                "INSERT INTO audit_log (ts,user_id,username,action,target,detail,ip) VALUES (?,?,?,?,?,?,?)",
                (datetime.now().isoformat(), user_id, username, action, target, detail, ip)
            )
            db.commit()
    except Exception: pass

def get_audit_log(limit: int = 200, user_id: int = None) -> list:
    with get_db() as db:
        if user_id:
            rows = db.execute(
                "SELECT * FROM audit_log WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit)).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]

# ── Trade history ──────────────────────────────────────
def trade_add(user_id: int, username: str, action: str,
              vk_id: int = None, vk_name: str = None,
              price: int = None, balance_after: int = None, detail: str = None):
    """Log a buy/sell trade."""
    try:
        with get_db() as db:
            db.execute(
                "INSERT INTO trade_history (ts,user_id,username,action,vk_id,vk_name,price,balance_after,detail) VALUES (?,?,?,?,?,?,?,?,?)",
                (datetime.now().isoformat(), user_id, username, action,
                 vk_id, vk_name, price, balance_after, detail)
            )
            db.commit()
    except Exception: pass

def get_trade_history(user_id: int = None, limit: int = 200) -> list:
    with get_db() as db:
        if user_id:
            rows = db.execute(
                "SELECT * FROM trade_history WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit)).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM trade_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]
