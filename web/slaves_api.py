"""
Slaves.su API wrapper
Портировано из rabstvo.py
"""
import os, json, time, base64, random, string, urllib.parse, requests
from datetime import datetime
from typing import Optional

BASE_URL   = "https://api.slaves.su/v1"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
SAVE_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".slaves_session")

# ── Состояние сессии ────────────────────────────────
sess = {
    "user_token":   "",
    "x_init_data":  "",
    "check_string": "",
    "vk_user_id":   0,
    "x_version":    "",
    "vk_first":     "",
    "vk_last":      "",
}

# ── Настройки ───────────────────────────────────────
cfg = {
    "max_price":        500_000,
    "max_buy":          20,
    "delay_buy":        1.5,
    "revenue_interval": 300,
    "auto_bonus":       True,
    "skip_clans":       False,
    "auto_revenue":     False,
}

# ── Статистика сессии ───────────────────────────────
stats = {
    "start":            None,
    "bought_count":     0,
    "bought_total":     0,
    "sold_count":       0,
    "sold_total":       0,
    "revenue_collected":0,
    "bonus_collected":  0,
    "errors":           0,
}

# ── Лог событий ────────────────────────────────────
event_log = []

def add_log(msg: str, level: str = "info"):
    ts = datetime.now().strftime("%H:%M:%S")
    event_log.append({"ts": ts, "msg": msg, "level": level})
    if len(event_log) > 200:
        event_log.pop(0)

# ══════════════════════════════════════════════════
#  КРИПТО
# ══════════════════════════════════════════════════

def _evp_kdf(password: bytes, salt: bytes):
    from Crypto.Hash import MD5
    d, prev = b"", b""
    while len(d) < 48:
        prev = MD5.new(prev + password + salt).digest()
        d += prev
    return d[:32], d[32:48]

def aes_encrypt(plaintext: str, passphrase: str) -> str:
    from Crypto.Cipher import AES
    salt    = os.urandom(8)
    key, iv = _evp_kdf(passphrase.encode("utf-8"), salt)
    cipher  = AES.new(key, AES.MODE_CBC, iv)
    data    = plaintext.encode("utf-8")
    pad     = 16 - len(data) % 16
    data   += bytes([pad] * pad)
    return base64.b64encode(b"Salted__" + salt + cipher.encrypt(data)).decode()

def int_to_base32(n: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuv"
    if n == 0: return "0"
    r = ""
    while n > 0: r = chars[n % 32] + r; n //= 32
    return r

def gen_uuid_token(n: int = 6) -> str:
    cs = string.ascii_letters + string.digits
    return "-".join("".join(random.choices(cs, k=4)) for _ in range(n))

def make_content_sign() -> str:
    if not sess["user_token"] or not sess["check_string"]: return ""
    ts  = int(time.time())
    msg = f"{sess['check_string']}:{gen_uuid_token(6)}:{ts}"
    enc = base64.b64encode(urllib.parse.quote(msg, safe="!~*'()").encode()).decode()
    return aes_encrypt(enc, sess["user_token"])

def make_temp_sign(action: str, *args) -> str:
    if not sess["user_token"]: return ""
    ts = int(time.time() * 1000)
    if action == "profile":          msg = f"profile:{ts}:{args[0]}"
    elif action == "profile_slaves": msg = f"profile_slaves:{ts}:{args[0]}"
    elif action == "buy":            msg = f"buy:{ts}:{args[0]}:{args[1]}"
    elif action == "sell":           msg = f"sell:{ts}:{args[0]}:sell_from_my_profile"
    elif action == "daily_bonus":    msg = f"daily_bonus:{ts}:0"
    elif action == "revenue":        msg = f"revenue:{ts}:0"
    else:                            msg = f"{action}:{ts}"
    return aes_encrypt(base64.b64encode(msg.encode()).decode(), sess["user_token"])

# ══════════════════════════════════════════════════
#  HTTP
# ══════════════════════════════════════════════════

def _base_headers(sign=True) -> dict:
    h = {
        "accept":     "application/json, text/plain, */*",
        "origin":     "https://slaves.su",
        "referer":    "https://slaves.su/",
        "x-version":  sess["x_version"],
        "user-agent": USER_AGENT,
    }
    if sign and sess["user_token"] and sess["check_string"]:
        h["content-sign"] = make_content_sign()
    return h

def _auth_headers() -> dict:
    return {
        "accept":      "application/json, text/plain, */*",
        "x-init-data": sess["x_init_data"],
        "origin":      "https://slaves.su",
        "referer":     "https://slaves.su/",
        "user-agent":  USER_AGENT,
    }

def _get(path: str, extra: dict = None, _retried=False) -> Optional[dict]:
    h = _base_headers()
    if extra: h.update(extra)
    try:
        r = requests.get(f"{BASE_URL}{path}", headers=h, timeout=10)
        if r.status_code in (401, 403) and not _retried and sess["x_init_data"]:
            if reauth(): return _get(path, extra, _retried=True)
        return r.json()
    except Exception as e:
        add_log(f"GET {path}: {e}", "err")
        stats["errors"] += 1
        return None

def _post(path: str, body: dict = None, extra: dict = None, _retried=False) -> Optional[dict]:
    h = _base_headers()
    if extra: h.update(extra)
    try:
        r = requests.post(f"{BASE_URL}{path}", headers=h, json=body or {}, timeout=10)
        if r.status_code in (401, 403) and not _retried and sess["x_init_data"]:
            if reauth(): return _post(path, body, extra, _retried=True)
        return r.json()
    except Exception as e:
        add_log(f"POST {path}: {e}", "err")
        stats["errors"] += 1
        return None

# ══════════════════════════════════════════════════
#  СЕССИЯ
# ══════════════════════════════════════════════════

def parse_xi_user(xi: str) -> dict:
    """Extract vk_user_id from x_init_data.
    Supports two formats:
    1. VK Mini Apps: vk_user_id=123&vk_app_id=...&sign=...
    2. WebApp: query_id=...&user={"id":123,...}
    """
    try:
        decoded = urllib.parse.unquote(xi)
        params  = urllib.parse.parse_qs(decoded, keep_blank_values=True)

        # Format 1: vk_user_id directly in params (VK Mini Apps)
        vk_uid_raw = params.get("vk_user_id", [None])[0]
        if vk_uid_raw and str(vk_uid_raw).isdigit():
            return {
                "vk_user_id": int(vk_uid_raw),
                "vk_first":   "",
                "vk_last":    "",
                "photo_100":  "",
                "photo_200":  "",
            }

        # Format 2: user={"id":123,"first_name":...} (WebApp)
        user_raw = params.get("user", [None])[0]
        if user_raw:
            import json as _json
            u = _json.loads(user_raw)
            return {
                "vk_user_id": int(u.get("id", 0)),
                "vk_first":   u.get("first_name", ""),
                "vk_last":    u.get("last_name",  ""),
                "photo_100":  u.get("photo_100",  ""),
                "photo_200":  u.get("photo_200",  ""),
            }

        # Format 3: viewer_id or user_id
        for key in ("viewer_id", "user_id", "id"):
            val = params.get(key, [None])[0]
            if val and str(val).isdigit():
                return {"vk_user_id": int(val), "vk_first":"", "vk_last":"", "photo_100":"", "photo_200":""}

    except Exception:
        pass
    return {}

def ensure_check_string():
    if sess["x_init_data"] and not sess["check_string"]:
        sess["check_string"] = urllib.parse.unquote(sess["x_init_data"])
    return bool(sess["check_string"])

def save_session():
    ensure_check_string()
    try:
        with open(SAVE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "user_token":   sess["user_token"],
                "x_init_data":  sess["x_init_data"],
                "check_string": sess["check_string"],
                "vk_user_id":   sess["vk_user_id"],
                "vk_first":     sess["vk_first"],
                "vk_last":      sess["vk_last"],
                "cfg":          cfg,
            }, f)
    except Exception: pass

def load_session() -> bool:
    try:
        with open(SAVE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        xi = data.get("x_init_data", "")
        cs = data.get("check_string", "")
        tk = data.get("user_token", "")
        if xi:
            sess["x_init_data"]  = xi
            sess["check_string"] = cs or urllib.parse.unquote(xi)
        if tk and xi:
            sess["user_token"] = tk
        sess["vk_user_id"] = data.get("vk_user_id", 0)
        sess["vk_first"]   = data.get("vk_first", "")
        sess["vk_last"]    = data.get("vk_last", "")
        saved_cfg = data.get("cfg", {})
        for k, v in saved_cfg.items():
            if k in cfg: cfg[k] = v
        if not isinstance(cfg.get("targets"), list): cfg["targets"] = []
        if sess["vk_user_id"]:
            sess["x_version"] = int_to_base32(sess["vk_user_id"])
        ensure_check_string()
        # Parse vk_user_id from xi if not already known
        if xi and not sess["vk_user_id"]:
            parsed = parse_xi_user(xi)
            if parsed.get("vk_user_id"):
                sess["vk_user_id"] = parsed["vk_user_id"]
                if not sess["vk_first"]: sess["vk_first"] = parsed.get("vk_first","")
                if not sess["vk_last"]:  sess["vk_last"]  = parsed.get("vk_last","")
        # Always set x_version if we have vk_user_id
        if sess["vk_user_id"]:
            sess["x_version"] = int_to_base32(sess["vk_user_id"])
        return bool(xi)
    except Exception:
        return False

def clear_session():
    for k in ("user_token", "x_init_data", "check_string"):
        sess[k] = ""
    sess["vk_user_id"] = 0
    sess["vk_first"]   = ""
    sess["vk_last"]    = ""
    try: os.remove(SAVE_FILE)
    except: pass

def reauth() -> bool:
    if not sess["x_init_data"]: return False
    ensure_check_string()
    # Ensure x_version is set before auth request
    if sess["vk_user_id"] and not sess["x_version"]:
        sess["x_version"] = int_to_base32(sess["vk_user_id"])
    old = sess["user_token"]
    sess["user_token"] = ""
    result = api_auth()
    if result and sess["user_token"]:
        add_log("Токен обновлён автоматически", "ok")
        return True
    sess["user_token"] = old
    add_log("Переавторизация не удалась", "err")
    return False

def is_ok() -> bool:
    return bool(sess["user_token"] and sess["check_string"])

def is_my_slave(owner_val) -> bool:
    if owner_val is None: return False
    if isinstance(owner_val, dict): return owner_val.get("vkid") == sess["vk_user_id"]
    if isinstance(owner_val, int): return owner_val == sess["vk_user_id"]
    return False

# ══════════════════════════════════════════════════
#  API МЕТОДЫ
# ══════════════════════════════════════════════════

def api_auth() -> Optional[dict]:
    ts = int(time.time() * 1000)
    try:
        r = requests.get(f"{BASE_URL}/auth?ts={ts}", headers=_auth_headers(), timeout=10)
        data = r.json()
        if "token" in data:
            sess["user_token"] = data["token"]
            save_session()
            return data
        return None
    except Exception as e:
        add_log(f"Auth error: {e}", "err")
        return None

def api_init() -> Optional[dict]:
    # Auto-parse uid from xi if missing
    if not sess["vk_user_id"] and sess["x_init_data"]:
        parsed = parse_xi_user(sess["x_init_data"])
        if parsed.get("vk_user_id"):
            sess["vk_user_id"] = parsed["vk_user_id"]
            if not sess["vk_first"]: sess["vk_first"] = parsed.get("vk_first","")
            if not sess["vk_last"]:  sess["vk_last"]  = parsed.get("vk_last","")
            if not sess.get("photo_100"): sess["photo_100"] = parsed.get("photo_100","")

    vkid  = sess.get("vk_user_id", 0)
    first = sess.get("vk_first", "User")
    last  = sess.get("vk_last", "")
    photo = sess.get("photo_100","")

    # x_version MUST be set before request
    if vkid:
        sess["x_version"] = int_to_base32(vkid)

    data = _post("/users/init", {
        "id":               vkid,
        "bdate_visibility": 0,
        "timezone":         3,
        "can_access_closed":True,
        "is_closed":        False,
        "first_name":       first,
        "last_name":        last,
        "sex":              2,
        "photo_100":        photo,
        "photo_200":        photo,
    })
    if data and "balance" in data:
        if data.get("vkid"):
            sess["vk_user_id"] = data["vkid"]
            sess["x_version"]  = int_to_base32(data["vkid"])
            sess["vk_first"]   = data.get("first_name", sess.get("vk_first",""))
            sess["vk_last"]    = data.get("last_name",  sess.get("vk_last",""))
    return data

def api_revenue() -> Optional[dict]:
    return _get("/users/revenue")

def api_daily_bonus() -> Optional[dict]:
    tmp = make_temp_sign("daily_bonus")
    return _get("/daily-bonus", {"x-temp-sign": tmp})

def api_my_slaves(last_vkid: int = 0) -> Optional[dict]:
    url = "/slaves/my?side=slaves"
    if last_vkid: url += f"&last_vkid={last_vkid}"
    return _get(url)

def api_profile(vkid: int) -> Optional[dict]:
    tmp = make_temp_sign("profile", vkid)
    return _get(f"/slaves/{vkid}/profile", {"x-temp-sign": tmp})

def api_profile_slaves(vkid: int, last_vkid: int = 0) -> Optional[dict]:
    tmp = make_temp_sign("profile_slaves", vkid)
    url = f"/slaves/{vkid}/profile_slaves"
    if last_vkid: url += f"?last_vkid={last_vkid}"
    return _get(url, {"x-temp-sign": tmp})

def api_buy(vkid: int, price: int) -> Optional[dict]:
    tmp = make_temp_sign("buy", vkid, price)
    return _post(f"/slaves/{vkid}/buy",
                 {"vkid": vkid, "hash": tmp, "job": ""},
                 {"x-temp-sign": tmp})

def api_sell(vkid: int) -> Optional[dict]:
    tmp = make_temp_sign("sell", vkid)
    return _get(f"/slaves/{vkid}/sell", {"x-temp-sign": tmp})

def api_top_members() -> Optional[list]:
    return _get("/top/members/get")

def api_top_king() -> Optional[dict]:
    return _get("/top/king")

def api_top_position() -> Optional[dict]:
    return _get("/top/position")

# ══════════════════════════════════════════════════
#  ХЕЛПЕРЫ
# ══════════════════════════════════════════════════

def fmt(n) -> str:
    if n is None: return "—"
    try: return f"{int(n):,}".replace(",", " ")
    except: return str(n)

def fmt_date(s: str) -> str:
    if not s: return "нет"
    try:
        dt = datetime.fromisoformat(s.replace("Z", ""))
        return dt.strftime("%d.%m %H:%M")
    except: return s[:16]

def owner_name(owner_val) -> str:
    if owner_val is None: return "Свободен"
    if isinstance(owner_val, dict):
        fn = owner_val.get("first_name", "")
        ln = owner_val.get("last_name", "")
        name = f"{fn} {ln}".strip()
        oid = owner_val.get("vkid", "")
        if oid == sess.get("vk_user_id"): return f"Ты ({name})"
        return f"{name} [id:{oid}]"
    if isinstance(owner_val, int):
        if owner_val == sess.get("vk_user_id"): return "Ты"
        return f"id:{owner_val}"
    return str(owner_val)

def uptime() -> str:
    if not stats["start"]: return "—"
    delta = datetime.now() - stats["start"]
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m, s   = divmod(rem, 60)
    if h > 0: return f"{h}ч {m}м"
    if m > 0: return f"{m}м {s}с"
    return f"{s}с"