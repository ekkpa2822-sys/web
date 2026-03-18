"""
╔══════════════════════════════════════════════════════╗
║           SLAVES BOT 2.0  —  Работники              ║
║   Reverse engineered from index-BUfLRVvj.js         ║
╚══════════════════════════════════════════════════════╝

Установка:
    pip install requests pycryptodome

Запуск:
    python slaves_bot.py

Получение x-init-data:
    Открой игру vk.com/app7804694 → F12 → Network
    Найди запрос auth?ts= → Headers → скопируй x-init-data
"""

import os, sys, json, time, base64, random, string, threading, urllib.parse, requests
from datetime import datetime
from typing import Optional

VK_USER_ID   = 806329630
VK_FIRST     = "Badwalk"
VK_LAST      = "Kur"
BASE_URL     = "https://api.slaves.su/v1"
SAVE_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".slaves_session")
INIT_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_slaves_init_data")
USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
VK_PHOTO     = (
    "https://sun9-72.userapi.com/s/v1/ig2/0nE1zls_36G8Qz0d38jM833f0x-tOdRKIkvAH5_jQA"
    "QcNIDayisju0LXGKxgICS9cLCrC7wDA-57hvCclvJ3UyYR.jpg?quality=95&crop=0,0,676,676"
    "&as=32x32,48x48,72x72,108x108,160x160,240x240,360x360,480x480,540x540,640x640"
    "&ava=1&u=3EntGg51Y_VTU6OgTbEwkhpnU7Qt6R0uoqli0smTfbI"
)

sess = {
    "user_token":    "",
    "x_init_data":   "",
    "check_string":  "",
    "vk_user_id":    VK_USER_ID,
    "x_version":     "",
    "profile":       None,
}

cfg = {
    "max_price":        500_000,
    "max_buy":          20,
    "delay_buy":        1.5,
    "targets":          [],
    "auto_revenue":     False,
    "revenue_interval": 300,
    "auto_bonus":       True,
    "skip_clans":       False,
}

stats = {
    "session_start":    None,
    "bought_count":     0,
    "bought_total":     0,
    "sold_count":       0,
    "sold_total":       0,
    "revenue_collected":0,
    "bonus_collected":  0,
    "revenue_ticks":    0,
    "errors":           0,
}

auto_thread = None
auto_stop_event = None
logs = []
LOG_MAX = 200

class C:
    RESET  = "\033[0m"; BOLD   = "\033[1m"; DIM    = "\033[2m"
    RED    = "\033[91m"; GREEN  = "\033[92m"; YELLOW = "\033[93m"
    BLUE   = "\033[94m"; PURPLE = "\033[95m"; CYAN   = "\033[96m"
    WHITE  = "\033[97m"; GRAY   = "\033[90m"

def c(color, text): return f"{color}{text}{C.RESET}"
def ok(text):   return c(C.GREEN,  f"✓ {text}")
def err(text):  return c(C.RED,    f"✗ {text}")
def warn(text): return c(C.YELLOW, f"! {text}")
def info(text): return c(C.CYAN,   f"→ {text}")

def log(msg: str, level: str = "info"):
    ts = datetime.now().strftime("%H:%M:%S")
    icons = {"ok": ok, "err": err, "warn": warn, "info": info}
    fn = icons.get(level, info)
    entry = f"{c(C.GRAY, ts)}  {fn(msg)}"
    logs.append(entry)
    if len(logs) > LOG_MAX: logs.pop(0)
    print(f"  {entry}")

def int_to_base32(n: int) -> str:
    chars = "0123456789abcdefghijklmnopqrstuv"
    if n == 0: return "0"
    r = ""
    while n > 0: r = chars[n % 32] + r; n //= 32
    return r

def gen_uuid_token(n: int = 6) -> str:
    charset = string.ascii_letters + string.digits
    return "-".join("".join(random.choices(charset, k=4)) for _ in range(n))

def format_num(n) -> str:
    if n is None: return "—"
    try: return f"{int(n):,}".replace(",", " ")
    except Exception: return str(n)

def format_date(s: str) -> str:
    if not s: return "нет"
    try:
        dt = datetime.fromisoformat(s.replace("Z", ""))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception: return s[:16]

def get_owner_id(owner_val) -> Optional[int]:
    if owner_val is None: return None
    if isinstance(owner_val, dict): return owner_val.get("vkid")
    if isinstance(owner_val, int): return owner_val
    return None

def get_owner_str(owner_val) -> str:
    if owner_val is None: return c(C.GREEN, "свободен")
    if isinstance(owner_val, dict):
        oid  = owner_val.get("vkid")
        fn   = owner_val.get("first_name", "")
        ln   = owner_val.get("last_name", "")
        name = f"{fn} {ln}".strip() or f"id:{oid}"
        if oid == sess["vk_user_id"]: return c(C.CYAN, f"ты  ({name})")
        return c(C.YELLOW, f"{name}  [id:{oid}]")
    if isinstance(owner_val, int):
        if owner_val == sess["vk_user_id"]: return c(C.CYAN, "ты")
        return c(C.YELLOW, f"id:{owner_val}")
    return str(owner_val)

def is_my_slave(owner_val) -> bool:
    return get_owner_id(owner_val) == sess["vk_user_id"]

def _evp_kdf(password: bytes, salt: bytes):
    from Crypto.Hash import MD5
    d, prev = b"", b""
    while len(d) < 48:
        prev = MD5.new(prev + password + salt).digest()
        d += prev
    return d[:32], d[32:48]

def aes_encrypt(plaintext: str, passphrase: str) -> str:
    try: from Crypto.Cipher import AES
    except ImportError: raise ImportError("Установи: pip install pycryptodome")
    salt    = os.urandom(8)
    key, iv = _evp_kdf(passphrase.encode("utf-8"), salt)
    cipher  = AES.new(key, AES.MODE_CBC, iv)
    data    = plaintext.encode("utf-8")
    pad     = 16 - len(data) % 16
    data   += bytes([pad] * pad)
    return base64.b64encode(b"Salted__" + salt + cipher.encrypt(data)).decode()

def make_content_sign() -> str:
    if not sess["user_token"] or not sess["check_string"]: return ""
    ts  = int(time.time())
    msg = f"{sess['check_string']}:{gen_uuid_token(6)}:{ts}"
    enc = base64.b64encode(urllib.parse.quote(msg, safe="!~*'()").encode()).decode()
    return aes_encrypt(enc, sess["user_token"])

def make_temp_sign(action: str, *args) -> str:
    if not sess["user_token"]: return ""
    ts = int(time.time() * 1000)
    if action == "profile":       msg = f"profile:{ts}:{args[0]}"
    elif action == "profile_slaves": msg = f"profile_slaves:{ts}:{args[0]}"
    elif action == "buy":         msg = f"buy:{ts}:{args[0]}:{args[1]}"
    elif action == "sell":        msg = f"sell:{ts}:{args[0]}:sell_from_my_profile"
    elif action == "daily_bonus": msg = f"daily_bonus:{ts}:0"
    elif action == "revenue":     msg = f"revenue:{ts}:0"
    else:                         msg = f"{action}:{ts}"
    return aes_encrypt(base64.b64encode(msg.encode()).decode(), sess["user_token"])

def base_headers(sign=True) -> dict:
    h = {
        "accept":    "application/json, text/plain, */*",
        "origin":    "https://slaves.su",
        "referer":   "https://slaves.su/",
        "x-version": sess["x_version"],
        "user-agent":USER_AGENT,
    }
    if sign and sess["user_token"]:
        if not sess["check_string"]: ensure_check_string()
        if sess["check_string"]: h["content-sign"] = make_content_sign()
    return h

def auth_headers() -> dict:
    return {
        "accept":      "application/json, text/plain, */*",
        "x-init-data": sess["x_init_data"],
        "origin":      "https://slaves.su",
        "referer":     "https://slaves.su/",
        "user-agent":  USER_AGENT,
    }

def ensure_check_string():
    if sess["x_init_data"] and not sess["check_string"]:
        sess["check_string"] = urllib.parse.unquote(sess["x_init_data"])
        log("check_string восстановлен из x-init-data", "warn")
    return bool(sess["check_string"])

def save_session():
    ensure_check_string()
    try:
        with open(SAVE_FILE, "w", encoding="utf-8") as f:
            json.dump({"user_token": sess["user_token"], "x_init_data": sess["x_init_data"], "check_string": sess["check_string"]}, f)
    except Exception: pass

def load_session() -> bool:
    loaded = False; saved_token = ""
    try:
        with open(SAVE_FILE, "r", encoding="utf-8") as f: data = json.load(f)
        xi = data.get("x_init_data", ""); cs = data.get("check_string", ""); tk = data.get("user_token", "")
        if xi: sess["x_init_data"] = xi; sess["check_string"] = cs if cs else urllib.parse.unquote(xi); loaded = True
        if tk: saved_token = tk
    except Exception: pass
    if not sess["x_init_data"]:
        for path in [INIT_FILE, os.path.join(os.getcwd(), "_slaves_init_data")]:
            try:
                with open(path, "r", encoding="utf-8") as f: xi = f.read().strip()
                if xi and len(xi) > 20:
                    sess["x_init_data"] = xi; sess["check_string"] = urllib.parse.unquote(xi)
                    log(f"x-init-data загружен из {os.path.basename(path)}", "ok"); loaded = True; break
            except Exception: continue
    if sess["x_init_data"] and saved_token: sess["user_token"] = saved_token
    elif saved_token and not sess["x_init_data"]:
        log("Токен найден, но x-init-data отсутствует", "warn")
    ensure_check_string()
    if sess["x_init_data"] and loaded: save_session()
    return loaded

def clear_session():
    sess["user_token"] = sess["x_init_data"] = sess["check_string"] = ""; sess["profile"] = None
    try: os.remove(SAVE_FILE)
    except Exception: pass

def reauth() -> bool:
    if not sess["x_init_data"]: return False
    log("Токен протух — переавторизуюсь через x-init-data...", "warn")
    ensure_check_string(); old_token = sess["user_token"]; sess["user_token"] = ""
    result = api_auth()
    if result and sess["user_token"]:
        ensure_check_string(); log(f"Новый токен получен: {sess['user_token'][:20]}...", "ok"); return True
    sess["user_token"] = old_token; log("Переавторизация не удалась", "err"); return False

def validate_token() -> bool:
    if not sess["user_token"]:
        if sess["x_init_data"]: return reauth()
        return False
    if not sess["check_string"]:
        if not ensure_check_string(): return False
    try:
        r = requests.get(f"{BASE_URL}/slaves/my?side=slaves", headers=base_headers(), timeout=6)
        if r.status_code == 200: return True
        return reauth()
    except Exception: return reauth()

def _get(path: str, extra: dict = None, _retried=False) -> Optional[dict]:
    h = base_headers()
    if extra: h.update(extra)
    try:
        r = requests.get(f"{BASE_URL}{path}", headers=h, timeout=10)
        if r.status_code in (401, 403) and not _retried and sess["x_init_data"]:
            if reauth(): return _get(path, extra, _retried=True)
        return r.json()
    except Exception as e: log(f"GET {path}: {e}", "err"); return None

def _post(path: str, body: dict = None, extra: dict = None, _retried=False) -> Optional[dict]:
    h = base_headers()
    if extra: h.update(extra)
    try:
        r = requests.post(f"{BASE_URL}{path}", headers=h, json=body or {}, timeout=10)
        if r.status_code in (401, 403) and not _retried and sess["x_init_data"]:
            if reauth(): return _post(path, body, extra, _retried=True)
        return r.json()
    except Exception as e: log(f"POST {path}: {e}", "err"); return None

def api_auth() -> Optional[dict]:
    ts = int(time.time() * 1000)
    try:
        r = requests.get(f"{BASE_URL}/auth?ts={ts}", headers=auth_headers(), timeout=10)
        data = r.json()
        if "token" in data: sess["user_token"] = data["token"]; save_session(); return data
        return None
    except Exception as e: log(f"Auth error: {e}", "err"); return None

def api_init() -> Optional[dict]:
    data = _post("/users/init", {
        "id": VK_USER_ID, "bdate_visibility": 0, "timezone": 3,
        "can_access_closed": True, "is_closed": False,
        "first_name": VK_FIRST, "last_name": VK_LAST, "sex": 2,
        "photo_100": VK_PHOTO + "&cs=100x100", "photo_200": VK_PHOTO + "&cs=200x200",
    })
    if data and "balance" in data: sess["profile"] = data
    return data

def api_revenue() -> Optional[dict]:    return _get("/users/revenue")
def api_my_slaves(side="slaves", last_vkid=0) -> Optional[dict]:
    url = f"/slaves/my?side={side}"
    if last_vkid: url += f"&last_vkid={last_vkid}"
    return _get(url)

def api_profile(vkid: int) -> Optional[dict]:
    tmp = make_temp_sign("profile", vkid)
    return _get(f"/slaves/{vkid}/profile", {"x-temp-sign": tmp})

def api_profile_slaves(vkid: int) -> Optional[dict]:
    tmp = make_temp_sign("profile_slaves", vkid)
    return _get(f"/slaves/{vkid}/profile_slaves", {"x-temp-sign": tmp})

def api_buy(vkid: int, price: int) -> Optional[dict]:
    tmp = make_temp_sign("buy", vkid, price)
    return _post(f"/slaves/{vkid}/buy", {"vkid": vkid, "hash": tmp, "job": ""}, {"x-temp-sign": tmp})

def api_buy_raw(vkid: int, sign_price, body_price=None) -> Optional[dict]:
    if body_price is None: body_price = sign_price
    tmp = make_temp_sign("buy", vkid, sign_price)
    return _post(f"/slaves/{vkid}/buy", {"vkid": vkid, "hash": tmp, "job": ""}, {"x-temp-sign": tmp})

def api_sell(vkid: int) -> Optional[dict]:
    tmp = make_temp_sign("sell", vkid)
    return _get(f"/slaves/{vkid}/sell", {"x-temp-sign": tmp})

def api_daily_bonus() -> Optional[dict]:
    tmp = make_temp_sign("daily_bonus")
    return _get("/daily-bonus", {"x-temp-sign": tmp})

def api_top(kind="holders") -> Optional[dict]: return _get(f"/top/{kind}")

def stats_reset():
    stats["session_start"] = datetime.now()
    for k in ["bought_count","bought_total","sold_count","sold_total","revenue_collected","bonus_collected","revenue_ticks","errors"]:
        stats[k] = 0

def stats_uptime() -> str:
    if not stats["session_start"]: return "—"
    delta = datetime.now() - stats["session_start"]
    h, rem = divmod(int(delta.total_seconds()), 3600); m, s = divmod(rem, 60)
    if h > 0: return f"{h}ч {m}мин {s}сек"
    elif m > 0: return f"{m}мин {s}сек"
    return f"{s}сек"

def _auto_revenue_worker():
    global auto_thread
    while not auto_stop_event.is_set():
        try:
            data = api_revenue()
            if data:
                added = data.get("add", data.get("coins_earned", 0))
                if isinstance(added, (int, float)) and added > 0:
                    stats["revenue_collected"] += int(added); stats["revenue_ticks"] += 1
                    log(f"[авто] Собрано {format_num(int(added))} монет", "ok")
            else: stats["errors"] += 1
        except Exception as e: log(f"[авто] Ошибка сбора: {e}", "err"); stats["errors"] += 1
        if cfg["auto_bonus"]:
            try:
                bdata = api_daily_bonus()
                if bdata:
                    msg = bdata.get("message", "")
                    if "уже" not in msg.lower() and "already" not in msg.lower():
                        stats["bonus_collected"] += 1; log("[авто] Ежедневный бонус получен!", "ok")
            except Exception: pass
        for _ in range(cfg["revenue_interval"]):
            if auto_stop_event.is_set(): break
            time.sleep(1)
    log("[авто] Авто-сбор остановлен", "info")

def auto_revenue_start():
    global auto_thread, auto_stop_event
    if auto_thread and auto_thread.is_alive(): log("Авто-сбор уже запущен", "warn"); return False
    auto_stop_event = threading.Event()
    auto_thread = threading.Thread(target=_auto_revenue_worker, daemon=True)
    auto_thread.start(); cfg["auto_revenue"] = True
    log(f"Авто-сбор запущен (каждые {cfg['revenue_interval']} сек)", "ok"); return True

def auto_revenue_stop():
    global auto_thread, auto_stop_event
    if auto_stop_event: auto_stop_event.set()
    if auto_thread: auto_thread.join(timeout=3); auto_thread = None
    cfg["auto_revenue"] = False; log("Авто-сбор остановлен", "info")

def auto_revenue_running() -> bool:
    return auto_thread is not None and auto_thread.is_alive()

W = 54

def line(char="─"): return c(C.GRAY, char * W)
def header(title: str):
    pad = W - 2 - len(title); lp = pad // 2; rp = pad - lp
    print(c(C.GRAY, "╔" + "═" * W + "╗"))
    print(c(C.GRAY, "║") + " " * lp + c(C.BOLD + C.WHITE, title) + " " * rp + c(C.GRAY, "║"))
    print(c(C.GRAY, "╚" + "═" * W + "╝"))

def section(title: str): print(); print(f"  {c(C.CYAN, '┌─')} {c(C.BOLD, title)}")
def row(label: str, value: str, icon: str = "│"):
    label_str = c(C.GRAY, f"{label}:")
    print(f"  {c(C.CYAN, icon)}  {label_str:<28} {value}")
def divider(): print(f"  {c(C.CYAN, '├' + '─' * (W - 2))}")
def close(): print(f"  {c(C.CYAN, '└' + '─' * (W - 2))}")

def status_bar():
    token_ok = sess["user_token"] != ""; xid_ok = sess["x_init_data"] != ""; cs_ok = sess["check_string"] != ""
    token_s = c(C.GREEN, "● токен") if token_ok else c(C.RED, "● нет токена")
    xid_s   = c(C.GREEN, "● xid")   if xid_ok   else c(C.RED, "● нет xid")
    cs_s    = c(C.GREEN, "● sign")  if cs_ok    else c(C.RED, "● нет sign")
    ver     = c(C.GRAY, sess["x_version"])
    print(f"\n  {c(C.GRAY, '─' * W)}")
    print(f"  {xid_s}  {token_s}  {cs_s}   ver: {ver}")
    print(f"  {c(C.GRAY, '─' * W)}\n")

def clear(): os.system("cls" if os.name == "nt" else "clear")
def prompt(text="Выбери действие") -> str:
    try: return input(f"\n  {c(C.CYAN, '❯')} {c(C.GRAY, text)}: ").strip().upper()
    except (KeyboardInterrupt, EOFError): return "0"

# ══════════════════════════════════════════════════════
#  ЭКРАНЫ
# ══════════════════════════════════════════════════════

def screen_menu():
    clear()
    header("SLAVES BOT  2.0")
    print()
    items = [
        ("1", "Мой профиль"),
        ("2", "Мои рабы"),
        ("3", "Собрать монеты"),
        ("4", "Ежедневный бонус"),
        ("5", "Автоскупка рабов"),
        ("6", "Авто-сбор монет" + (" " + c(C.GREEN, "●") if auto_revenue_running() else "")),
        ("7", "Статистика сессии"),
        ("9", "Поиск по VK ID"),
        ("P", "Сканер диапазона ID  🔍"),
        ("Q", "Парсер дешёвых игроков  💰"),
        ("B", "Ручная покупка"),
        ("X", "Тест цены (эксперимент)"),
        ("L", "Логи"),
        ("S", "Настройки"),
        ("M", "Ввести x-init-data"),
        ("0", "Выход"),
    ]
    for key, label in items:
        key_s = c(C.CYAN + C.BOLD, f" {key} "); label_s = c(C.WHITE, label)
        print(f"  {c(C.GRAY, '│')} {key_s}  {label_s}")
    print(f"  {c(C.GRAY, '└' + '─' * (W - 2))}")
    status_bar()

def screen_profile():
    clear(); header("МОЙ ПРОФИЛЬ"); print()
    log("Загружаю профиль...")
    data = api_init()
    if not data or "balance" not in data:
        log("Не удалось загрузить профиль", "err"); input(c(C.GRAY, "\n  Enter для возврата...")); return
    name = f"{data.get('first_name','')} {data.get('last_name','')}".strip()
    section(c(C.BOLD + C.WHITE, name) + c(C.GRAY, f"  (id: {data.get('vkid','?')})"))
    divider()
    row("Монеты",  c(C.YELLOW, format_num(data.get("balance"))) + " 🪙")
    row("Доход",   c(C.GREEN,  format_num(data.get("income")))  + " /ч")
    row("Рабов",   c(C.WHITE,  format_num(data.get("slaves_count"))))
    row("Работа",  c(C.PURPLE, data.get("job", "—")))
    row("Заработано", c(C.GRAY, format_num(data.get("earned"))))
    divider()
    owner_raw = data.get("owner"); owner_id = get_owner_id(owner_raw)
    if owner_id: row("Хозяин", get_owner_str(owner_raw), "│")
    else: row("Статус", c(C.GREEN, "свободен"), "│")
    shield = data.get("shield"); fetters = data.get("fetters")
    if shield:  row("Щит до",   c(C.BLUE,   format_date(shield)))
    if fetters: row("Оковы до", c(C.YELLOW, format_date(fetters)))
    group = data.get("group")
    if group and isinstance(group, dict): row("Клан", c(C.CYAN, group.get("name", "—")))
    divider()
    bonus = data.get("daily_bonus", {}); can = bonus.get("can_collect", 0); days = bonus.get("collecting_days", 0)
    row("Бонус", c(C.GREEN, f"день {days}, {'доступен ✓' if can else 'получен'}"))
    close()
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_my_slaves():
    clear(); header("МОИ РАБЫ"); print()
    log("Загружаю список рабов...")
    data = api_my_slaves(); slaves = []
    if isinstance(data, dict): slaves = data.get("slaves", [])
    elif isinstance(data, list): slaves = data
    slaves = [s for s in slaves if s.get("vkid") != sess["vk_user_id"]]
    if not slaves: log("Рабов нет", "warn"); input(c(C.GRAY, "\n  Enter для возврата...")); return

    total_cost   = sum(s.get("cost", 0) for s in slaves)
    raw_income   = sum(s.get("salary", s.get("income", 0)) for s in slaves)

    # ── Определяем буст-множитель через профиль ──
    boost_mult   = 1.0
    boost_label  = ""
    profile_data = api_init()
    if profile_data and isinstance(profile_data, dict):
        real_income = profile_data.get("income", 0)
        if raw_income > 0 and real_income > 0:
            boost_mult = real_income / raw_income
            if boost_mult >= 1.95:
                boost_label = c(C.GREEN + C.BOLD, "  ×2 🚀")
            elif boost_mult > 1.05:
                boost_label = c(C.GREEN, f"  ×{boost_mult:.1f} 🚀")
        display_income = real_income
    else:
        display_income = raw_income

    sorted_slaves = sorted(slaves, key=lambda x: x.get("cost", 0), reverse=True)

    section(f"Всего: {c(C.WHITE+C.BOLD, str(len(slaves)))} раб(ов)  │  "
            f"Стоимость: {c(C.YELLOW, format_num(total_cost))} 🪙  │  "
            f"Доход: {c(C.GREEN, format_num(display_income))}/ч{boost_label}")
    print(f"  {c(C.CYAN, '│')}")

    if boost_mult > 1.05:
        print(f"  {c(C.CYAN, '│')}  {c(C.GRAY, f'Базовый: {format_num(raw_income)}/ч')}  "
              f"{c(C.GREEN, f'→ с бустом: {format_num(display_income)}/ч')}")
    print(f"  {c(C.CYAN, '│')}")

    print(f"  {c(C.CYAN, '│')}  {c(C.GRAY, f'{'Имя':<22}')} {c(C.GRAY, f'{'Покупная цена':>14}')}  "
          f"{c(C.GRAY, f'{'Доход/ч':>8}')}  {c(C.GRAY, f'{'Работа':<16}')}")
    print(f"  {c(C.CYAN, '├' + '─' * (W - 2))}")

    for s in sorted_slaves:
        name       = f"{s.get('first_name','')} {s.get('last_name','')}".strip() or f"id:{s.get('vkid','?')}"
        cost       = s.get("cost", 0)
        s_raw_inc  = s.get("salary", s.get("income", 0))
        s_income   = int(s_raw_inc * boost_mult) if boost_mult > 1.05 else s_raw_inc

        if cost >= 1_000_000: cost_c = C.PURPLE
        elif cost >= 100_000: cost_c = C.YELLOW
        elif cost >= 10_000:  cost_c = C.WHITE
        else:                 cost_c = C.GRAY

        inc_s = c(C.GREEN, f"{format_num(s_income):>8}") if s_income > 0 else c(C.GRAY, f"{'—':>8}")
        job   = s.get("job", "—")
        print(f"  {c(C.CYAN, '│')}  {name[:22]:<22} {c(cost_c, f'{format_num(cost):>14}')}  {inc_s}  {c(C.GRAY, f'{job[:16]:<16}')}")

    close()
    print(f"\n  {c(C.GRAY, 'Всего стоимость:')} {c(C.YELLOW+C.BOLD, format_num(total_cost))} монет")
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_revenue():
    clear(); header("СБОР МОНЕТ"); print()
    log("Отправляю запрос на сбор монет...")
    data = api_revenue()
    if not data:
        log("Нет ответа от сервера", "err"); stats["errors"] += 1
    elif not isinstance(data, dict):
        log(f"Неожиданный ответ сервера: {data}", "warn"); stats["errors"] += 1
    elif data.get("error") or data.get("message"):
        log(str(data.get("error") or data.get("message")), "warn")
    else:
        added = data.get("add", data.get("coins_earned", data.get("balance", 0)))
        if isinstance(added, (int, float)) and added > 0:
            stats["revenue_collected"] += int(added); stats["revenue_ticks"] += 1
        log(f"Получено: {c(C.YELLOW+C.BOLD, format_num(added))} монет", "ok")
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_daily_bonus():
    clear(); header("ЕЖЕДНЕВНЫЙ БОНУС"); print()
    log("Получаю бонус...")
    data = api_daily_bonus()
    if not data:
        log("Нет ответа от сервера", "err")
    elif not isinstance(data, dict):
        log(f"Неожиданный ответ сервера: {data}", "warn")
    elif data.get("error") or data.get("message"):
        msg = str(data.get("error") or data.get("message", ""))
        log(msg, "warn" if ("уже" in msg.lower() or "already" in msg.lower()) else "ok")
    else:
        added = data.get("add", data.get("coins_earned", data.get("bonus", data.get("balance", ""))))
        stats["bonus_collected"] += 1
        log(f"Бонус получен: {c(C.YELLOW+C.BOLD, format_num(added)) if added else str(data)}", "ok")
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_lookup():
    clear(); header("ПОИСК ПО VK ID"); print()
    vkid_str = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, 'VK ID')}: ").strip()
    if not vkid_str.isdigit(): log("Неверный ID", "err"); input(c(C.GRAY, "\n  Enter для возврата...")); return
    vkid = int(vkid_str)
    log(f"Загружаю профиль {vkid}..."); profile = api_profile(vkid)
    log(f"Загружаю рабов {vkid}..."); slaves_data = api_profile_slaves(vkid)
    if not profile: log("Профиль не найден", "err"); input(c(C.GRAY, "\n  Enter для возврата...")); return
    u = profile.get("user", profile)
    name = f"{u.get('first_name','')} {u.get('last_name','')}".strip() or str(vkid)
    is_me = (vkid == sess["vk_user_id"]); owner_raw = u.get("owner") or u.get("master_id")
    is_mine = is_my_slave(owner_raw); cost = u.get("cost", u.get("sale_price", 0))
    tag = c(C.CYAN, "  (это ты)") if is_me else ""
    section(c(C.BOLD + C.WHITE, name) + c(C.GRAY, f"  [id:{vkid}]") + tag); divider()
    row("Цена", c(C.YELLOW + C.BOLD, format_num(cost)) + " 🪙"); row("Хозяин", get_owner_str(owner_raw))
    balance = u.get("balance"); income = u.get("income", u.get("salary")); slaves_count = u.get("slaves_count")
    if balance is not None: row("Монеты", c(C.YELLOW, format_num(balance)) + " 🪙")
    if income is not None:  row("Доход",  c(C.GREEN,  format_num(income))  + " /ч")
    if slaves_count is not None: row("Рабов", c(C.WHITE, format_num(slaves_count)))
    row("Работа", c(C.PURPLE, u.get("job", "—")))
    group = u.get("group") or u.get("squad") or u.get("clan")
    if group and isinstance(group, dict): row("Клан", c(C.CYAN, group.get("name", str(group))))
    else: row("Клан", c(C.GRAY, "нет"))
    row("Щит до",   c(C.BLUE,   format_date(u.get("shield")))   if u.get("shield")   else c(C.GRAY, "нет"))
    row("Оковы до", c(C.YELLOW, format_date(u.get("fetters"))) if u.get("fetters") else c(C.GRAY, "нет"))
    divider()
    slave_list = []
    if isinstance(slaves_data, dict): slave_list = slaves_data.get("slaves", [])
    elif isinstance(slaves_data, list): slave_list = slaves_data
    if slave_list:
        total_slave_cost = sum(s.get("cost", 0) for s in slave_list)
        print(f"  {c(C.CYAN, '│')}  {c(C.BOLD, 'Рабы:')} {c(C.WHITE, str(len(slave_list)))} чел.  суммарно {c(C.YELLOW, format_num(total_slave_cost))} 🪙")
        print(f"  {c(C.CYAN, '│')}")
        for s in sorted(slave_list, key=lambda x: x.get("cost", 0), reverse=True)[:15]:
            sname = f"{s.get('first_name','')} {s.get('last_name','')}".strip()[:22]
            _scost = format_num(s.get("cost", 0))
            _sjob  = (s.get("job") or "—")[:14]
            _row   = f"  {sname:<22}  {_scost:>10}  {_sjob}"
            print(f"  {c(C.CYAN, chr(9474))}    {_row}")
        if len(slave_list) > 15: print(f"  {c(C.CYAN, '│')}    {c(C.GRAY, f'... и ещё {len(slave_list)-15}')}")
    else: print(f"  {c(C.CYAN, '│')}  {c(C.GRAY, 'Рабов нет')}")
    close()
    if not is_me and not is_mine and isinstance(cost, int) and cost > 0:
        print()
        ans = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, f'Купить {name} за {format_num(cost)} монет? (y/n)')}: ").strip().lower()
        if ans == "y":
            result = api_buy(vkid, cost); print()
            if result:
                err_msg = result.get("error") or result.get("message")
                if err_msg: log(err_msg, "err"); stats["errors"] += 1
                else: log(f"Куплен: {name}", "ok"); stats["bought_count"] += 1; stats["bought_total"] += cost
            else: log("Нет ответа", "err"); stats["errors"] += 1
    elif is_mine: print(f"\n  {c(C.CYAN, '⛓  Уже твой раб')}")
    elif is_me:   print(f"\n  {c(C.CYAN, 'Это твой аккаунт')}")
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_manual_buy():
    clear(); header("РУЧНАЯ ПОКУПКА"); print()
    vkid_str = input(f"  {c(C.CYAN, chr(10075))} {c(C.GRAY, 'VK ID раба')}: ").strip()
    if not vkid_str.isdigit(): log("Неверный ID", "err"); input(c(C.GRAY, "\n  Enter для возврата...")); return
    vkid = int(vkid_str); log(f"Загружаю профиль {vkid}..."); profile = api_profile(vkid)
    cost = None; name = str(vkid)
    if profile:
        u = profile.get("user", profile); name = f"{u.get('first_name','')} {u.get('last_name','')}".strip() or str(vkid)
        cost = u.get("cost", u.get("sale_price")); owner = u.get("owner") or u.get("master_id")
        print(); section(c(C.WHITE + C.BOLD, name)); row("Цена", c(C.YELLOW + C.BOLD, format_num(cost)) + " монет"); row("Хозяин", get_owner_str(owner)); close()
    else: log("Профиль не загрузился, введи цену вручную", "warn")
    price_str = input(f"\n  {c(C.CYAN, chr(10075))} {c(C.GRAY, f'Цена (Enter = {format_num(cost)})')}: ").strip()
    if price_str.isdigit(): cost = int(price_str)
    elif cost is None: log("Цена не указана", "err"); input(c(C.GRAY, "\n  Enter для возврата...")); return
    print()
    ans = input(f"  {c(C.CYAN, chr(10075))} {c(C.GRAY, f'Купить {name} за {format_num(cost)}? (y/n)')}: ").strip().lower()
    if ans != "y": return
    log(f"Покупаю {name} за {format_num(cost)}..."); result = api_buy(vkid, cost); print()
    if result:
        err_msg = result.get("error") or result.get("message")
        if err_msg: log(err_msg, "err"); stats["errors"] += 1
        else: log(f"Куплен: {name}!", "ok"); stats["bought_count"] += 1; stats["bought_total"] += cost
        print(f"\n  {c(C.GRAY, 'Ответ:')} {json.dumps(result, ensure_ascii=False)[:200]}")
    else: log("Нет ответа от сервера", "err"); stats["errors"] += 1
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_autobuy():
    clear(); header("АВТОСКУПКА РАБОВ ЦЕЛИ"); print()
    if not sess["user_token"]:
        log("Нет токена — введи x-init-data через M", "err")
        input(c(C.GRAY, "\n  Enter для возврата...")); return
    targets = cfg["targets"]
    if not targets:
        log("Список целей пуст", "warn")
        print(f"\n  {c(C.GRAY, 'Добавь VK ID владельцев в настройках (S → Цели)')}")
        input(c(C.GRAY, "\n  Enter для возврата...")); return

    section("Параметры")
    row("Целей (владельцев)", c(C.WHITE,  str(len(targets))))
    row("Макс. цена раба",    c(C.YELLOW, format_num(cfg["max_price"])) + " 🪙")
    row("Макс. покупок",      c(C.WHITE,  str(cfg["max_buy"])))
    row("Задержка",           c(C.GRAY,   f"{cfg['delay_buy']} сек"))
    row("Пропуск кланов",     c(C.GREEN if cfg["skip_clans"] else C.GRAY,
                               "вкл" if cfg["skip_clans"] else "выкл"))
    close()
    print(f"\n  {c(C.GRAY, 'Логика: берём каждого владельца → смотрим его рабов → проверяем цену → покупаем.')}")
    ans = input(f"\n  {c(C.CYAN, '❯')} {c(C.GRAY, 'Запустить? (y/n)')}: ").strip().lower()
    if ans != "y": return

    print()
    bought = 0; skipped = 0; errors = 0; total_slaves_found = 0

    for t_idx, owner_vkid in enumerate(targets, 1):
        if bought >= cfg["max_buy"]:
            log(f"Лимит покупок достигнут ({cfg['max_buy']})", "warn"); break

        t_prefix = c(C.CYAN, f"[владелец {t_idx}/{len(targets)}]")

        # Получаем имя владельца
        owner_profile = api_profile(owner_vkid)
        if owner_profile:
            ou = owner_profile.get("user", owner_profile)
            owner_name = f"{ou.get('first_name','')} {ou.get('last_name','')}".strip() or str(owner_vkid)
        else:
            owner_name = str(owner_vkid)

        print(f"\n  {t_prefix} {c(C.WHITE + C.BOLD, owner_name)}  {c(C.GRAY, f'id:{owner_vkid}')}")
        print(f"  {c(C.GRAY, '  Загружаю список рабов...')}", end=" ", flush=True)

        slaves_data = api_profile_slaves(owner_vkid)

        # Парсим список рабов
        slave_list = []
        if isinstance(slaves_data, dict):
            slave_list = slaves_data.get("slaves", slaves_data.get("items", []))
        elif isinstance(slaves_data, list):
            slave_list = slaves_data

        if not slave_list:
            print(c(C.GRAY, "рабов нет"))
            continue

        print(c(C.WHITE, f"{len(slave_list)} рабов"))
        total_slaves_found += len(slave_list)

        # Сортируем по цене (дешевле — первые)
        slave_list_sorted = sorted(slave_list, key=lambda s: s.get("cost", 0))

        for s_idx, slave in enumerate(slave_list_sorted, 1):
            if bought >= cfg["max_buy"]:
                log(f"Лимит покупок достигнут ({cfg['max_buy']})", "warn"); break

            # Получаем vkid раба
            slave_vkid = slave.get("vkid") or slave.get("id") or slave.get("vk_id")
            if not slave_vkid:
                skipped += 1; continue
            slave_vkid = int(slave_vkid)

            # Себя не покупаем
            if slave_vkid == sess["vk_user_id"]:
                skipped += 1; continue

            s_prefix = c(C.GRAY, f"    [{s_idx}/{len(slave_list_sorted)}]")

            # Свежая цена из профиля
            profile = api_profile(slave_vkid)
            if not profile:
                print(f"  {s_prefix}  {c(C.GRAY, str(slave_vkid))} — {c(C.RED, 'нет ответа')}")
                errors += 1; time.sleep(cfg["delay_buy"]); continue

            u    = profile.get("user", profile)
            name = f"{u.get('first_name','')} {u.get('last_name','')}".strip() or str(slave_vkid)
            cost = u.get("cost", u.get("sale_price", 0))
            owner_raw = u.get("owner") or u.get("master_id")

            # Уже мой?
            if is_my_slave(owner_raw):
                print(f"  {s_prefix}  {c(C.GRAY, name[:22]):<22}  {c(C.CYAN, '⛓ уже твой')}")
                skipped += 1; time.sleep(cfg["delay_buy"]); continue

            # Цена валидна?
            if not isinstance(cost, (int, float)) or cost <= 0:
                print(f"  {s_prefix}  {c(C.GRAY, name[:22]):<22}  {c(C.GRAY, 'цена неизвестна')}")
                skipped += 1; time.sleep(cfg["delay_buy"]); continue

            cost = int(cost)

            # Выше лимита?
            if cost > cfg["max_price"]:
                print(f"  {s_prefix}  {name[:22]:<22}  {c(C.YELLOW, format_num(cost))} 🪙  {c(C.GRAY, '> лимит')}")
                skipped += 1; time.sleep(cfg["delay_buy"]); continue

            # Клан?
            if cfg["skip_clans"]:
                group = u.get("group") or u.get("squad") or u.get("clan")
                if group:
                    print(f"  {s_prefix}  {c(C.GRAY, name[:22]):<22}  {c(C.GRAY, 'в клане, пропуск')}")
                    skipped += 1; time.sleep(cfg["delay_buy"]); continue

            # Покупаем
            print(f"  {s_prefix}  {c(C.WHITE, name[:22]):<22}  "
                  f"{c(C.YELLOW, format_num(cost))} 🪙  ...",
                  end=" ", flush=True)

            result = api_buy(slave_vkid, cost)
            if result:
                err_msg = result.get("error") or result.get("message")
                if err_msg:
                    print(c(C.RED, f"✗ {err_msg}"))
                    errors += 1; stats["errors"] += 1
                else:
                    new_bal = result.get("new_balance", "?")
                    print(c(C.GREEN, f"✓ куплен!") + c(C.GRAY, f"  баланс: {format_num(new_bal)} 🪙"))
                    bought += 1; stats["bought_count"] += 1; stats["bought_total"] += cost
            else:
                print(c(C.RED, "✗ нет ответа"))
                errors += 1; stats["errors"] += 1

            time.sleep(cfg["delay_buy"])

    print()
    section("Итог")
    row("Владельцев проверено", c(C.WHITE,  str(len(targets))))
    row("Рабов найдено",        c(C.WHITE,  str(total_slaves_found)))
    row("Куплено",              c(C.GREEN,  str(bought)))
    row("Пропущено",            c(C.GRAY,   str(skipped)))
    row("Ошибок",               c(C.RED if errors else C.GRAY, str(errors)))
    if bought > 0:
        row("Потрачено",        c(C.YELLOW, format_num(stats["bought_total"])) + " 🪙")
    close()
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_stats():
    clear(); header("СТАТИСТИКА СЕССИИ"); print()
    section("Время")
    row("Старт сессии", c(C.WHITE, stats["session_start"].strftime("%d.%m.%Y %H:%M:%S") if stats["session_start"] else "—"))
    row("Аптайм", c(C.CYAN, stats_uptime())); divider()
    section("Покупки")
    row("Куплено рабов", c(C.GREEN, str(stats["bought_count"]))); row("Потрачено", c(C.YELLOW, format_num(stats["bought_total"])) + " 🪙")
    if stats["bought_count"] > 0: row("Средняя цена", c(C.GRAY, format_num(stats["bought_total"] // stats["bought_count"])) + " 🪙")
    divider()
    section("Сбор")
    row("Собрано монет", c(C.YELLOW, format_num(stats["revenue_collected"])) + " 🪙"); row("Сборов", c(C.GRAY, str(stats["revenue_ticks"]))); row("Бонусов", c(C.GREEN, str(stats["bonus_collected"]))); divider()
    section("Авто-сбор"); running = auto_revenue_running()
    row("Статус", c(C.GREEN, "● работает") if running else c(C.RED, "● выключен")); row("Интервал", c(C.GRAY, f"{cfg['revenue_interval']} сек")); row("Авто-бонус", c(C.GREEN, "вкл") if cfg["auto_bonus"] else c(C.GRAY, "выкл")); divider()
    net = stats["revenue_collected"] - stats["bought_total"] + stats["sold_total"]
    row("Нетто", c(C.GREEN if net >= 0 else C.RED, format_num(net)) + " 🪙"); row("Ошибок", c(C.RED if stats["errors"] else C.GRAY, str(stats["errors"]))); close()
    input(c(C.GRAY, "\n  Enter для возврата..."))

def screen_auto_revenue():
    clear(); header("АВТО-СБОР МОНЕТ"); print(); running = auto_revenue_running()
    section("Статус"); row("Авто-сбор", c(C.GREEN, "● работает") if running else c(C.RED, "● выключен"))
    row("Интервал", c(C.GRAY, f"{cfg['revenue_interval']} сек ({cfg['revenue_interval']//60} мин)"))
    row("Авто-бонус", c(C.GREEN, "вкл") if cfg["auto_bonus"] else c(C.GRAY, "выкл"))
    row("Собрано", c(C.YELLOW, format_num(stats["revenue_collected"])) + " 🪙"); close(); print()
    if running: print(f"  {c(C.WHITE, '1')}  Остановить авто-сбор")
    else: print(f"  {c(C.WHITE, '1')}  Запустить авто-сбор")
    print(f"  {c(C.WHITE, '2')}  Изменить интервал (сейчас {cfg['revenue_interval']} сек)")
    print(f"  {c(C.WHITE, '3')}  Авто-бонус: {'выкл → вкл' if not cfg['auto_bonus'] else 'вкл → выкл'}")
    print(f"  {c(C.WHITE, '0')}  Назад")
    choice = prompt("Выбери действие")
    if choice == "1":
        if running: auto_revenue_stop()
        else:
            if not sess["user_token"]: log("Нет токена — введи x-init-data через M", "err")
            else: auto_revenue_start()
    elif choice == "2":
        v = input(f"  {c(C.GRAY, 'Интервал (секунд)')}: ").strip()
        if v.isdigit() and int(v) >= 10: cfg["revenue_interval"] = int(v); log(f"Интервал изменён на {v} сек", "ok")
        else: log("Минимум 10 секунд", "warn")
    elif choice == "3": cfg["auto_bonus"] = not cfg["auto_bonus"]; log(f"Авто-бонус: {'вкл' if cfg['auto_bonus'] else 'выкл'}", "ok")
    input(c(C.GRAY, "\n  Enter для возврата..."))

# ══════════════════════════════════════════════════════
#  СКАНЕР ДИАПАЗОНА VK ID (оригинальный — проверяет рабов)
# ══════════════════════════════════════════════════════

scanner = {
    "id_from": 1, "id_to": 1000, "max_price": 50_000, "delay": 0.8,
    "auto_buy": False, "skip_shielded": True, "skip_clans": True, "skip_owned": True,
    "last_scanned": 0, "running": False,
    "total_scanned": 0, "total_found": 0, "total_bought": 0, "total_spent": 0,
    "total_skipped": 0, "total_errors": 0, "total_empty": 0, "found_list": [],
}

def _scanner_reset_stats():
    scanner["total_scanned"] = scanner["total_found"] = scanner["total_bought"] = 0
    scanner["total_spent"] = scanner["total_skipped"] = scanner["total_errors"] = scanner["total_empty"] = 0
    scanner["found_list"] = []

def _scanner_progress_bar(current, total, width=36):
    if total <= 0: return c(C.GRAY, "[" + "?" * width + "]")
    pct = min(current / total, 1.0); filled = int(width * pct); empty = width - filled
    bar = c(C.GREEN, "█" * filled) + c(C.GRAY, "░" * empty)
    return f"[{bar}] {c(C.WHITE + C.BOLD, f'{pct*100:5.1f}%')}"

def _scanner_eta(scanned, total, elapsed_sec):
    if scanned <= 0 or elapsed_sec <= 0: return "—"
    rate = scanned / elapsed_sec; remaining = total - scanned
    if rate <= 0: return "∞"
    eta_sec = remaining / rate
    if eta_sec > 3600: return f"{eta_sec/3600:.1f}ч"
    elif eta_sec > 60: return f"{int(eta_sec//60)}мин {int(eta_sec%60)}сек"
    return f"{int(eta_sec)}сек"

def _scanner_display_found(entry, index, total):
    print(); print(f"  {'═' * 50}")
    print(f"{c(C.GREEN + C.BOLD, f'  ★ НАЙДЕН [{index}/{total}]')}"); print(f"  {'═' * 50}"); print()
    section(c(C.WHITE + C.BOLD, entry.get("name","???")) + c(C.GRAY, f"  [vk.com/id{entry.get('vkid',0)}]")); divider()
    row("Цена", c(C.YELLOW + C.BOLD, format_num(entry.get("cost",0))) + " 🪙"); row("Хозяин", entry.get("owner_str","?"))
    if entry.get("income"): row("Доход", c(C.GREEN, format_num(entry.get("income"))) + " /ч")
    if entry.get("slaves_count"): row("Рабов", c(C.WHITE, format_num(entry.get("slaves_count"))))
    if entry.get("job","—") != "—": row("Работа", c(C.PURPLE, entry.get("job")))
    if entry.get("group_name"): row("Клан", c(C.CYAN, entry.get("group_name")))
    if entry.get("shield"): row("Щит до", c(C.BLUE, format_date(entry.get("shield"))))
    if entry.get("fetters"): row("Оковы до", c(C.YELLOW, format_date(entry.get("fetters"))))
    income = entry.get("income",0); cost = entry.get("cost",0)
    if income and income > 0 and cost > 0:
        ph = cost / income
        if ph < 1: ps = c(C.GREEN + C.BOLD, f"{ph*60:.0f} мин")
        elif ph < 24: ps = c(C.GREEN, f"{ph:.1f} ч")
        elif ph < 168: ps = c(C.YELLOW, f"{ph/24:.1f} дн")
        else: ps = c(C.RED, f"{ph/24:.0f} дн")
        row("Окупаемость", ps)
    close()

def _scanner_run():
    id_from = scanner["id_from"]; id_to = scanner["id_to"]
    max_price = scanner["max_price"]; delay = scanner["delay"]; auto_buy = scanner["auto_buy"]
    total_range = id_to - id_from + 1
    if total_range <= 0: log("Неверный диапазон", "err"); return
    resume_from = id_from
    if scanner["last_scanned"] > 0 and id_from <= scanner["last_scanned"] < id_to:
        print(); print(f"  {c(C.YELLOW, '!')} Последний просканированный ID: {c(C.WHITE, str(scanner['last_scanned']))}")
        ans = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, 'Продолжить с него? (y/n)')}: ").strip().lower()
        if ans == "y": resume_from = scanner["last_scanned"] + 1; total_range = id_to - resume_from + 1; log(f"Продолжаю с ID {resume_from}", "info")
        else: _scanner_reset_stats()
    if scanner["total_scanned"] == 0: _scanner_reset_stats()
    print(); print(f"  {c(C.CYAN, '┌──────────────────────────────────────────────────')}")
    print(f"  {c(C.CYAN, '│')}  {c(C.BOLD + C.WHITE, '🔍  СКАНЕР ЗАПУЩЕН')}")
    print(f"  {c(C.CYAN, '│')}  Диапазон:   {c(C.WHITE, f'{format_num(resume_from)} → {format_num(id_to)}')}  ({c(C.GRAY, f'{format_num(total_range)} ID')})")
    print(f"  {c(C.CYAN, '│')}  Макс. цена: {c(C.YELLOW, format_num(max_price))} 🪙")
    print(f"  {c(C.CYAN, '│')}  Задержка:   {c(C.GRAY, f'{delay} сек')}")
    print(f"  {c(C.CYAN, '│')}  Авто-покупка: {c(C.GREEN, 'ДА') if auto_buy else c(C.GRAY, 'нет')}")
    print(f"  {c(C.CYAN, '│')}  {c(C.GRAY, 'Ctrl+C для остановки')}")
    print(f"  {c(C.CYAN, '└──────────────────────────────────────────────────')}"); print()
    scanner["running"] = True; start_time = time.time(); scan_count = 0
    try:
        for vkid in range(resume_from, id_to + 1):
            if not scanner["running"]: break
            scan_count += 1; scanner["total_scanned"] += 1; scanner["last_scanned"] = vkid
            elapsed = time.time() - start_time
            if scan_count == 1 or scan_count % 5 == 0:
                bar = _scanner_progress_bar(scan_count, total_range); eta = _scanner_eta(scan_count, total_range, elapsed)
                rate = scan_count / elapsed if elapsed > 0 else 0; found_s = c(C.GREEN + C.BOLD, str(scanner["total_found"])) if scanner["total_found"] > 0 else c(C.GRAY, "0")
                print(f"\r  {bar}  ID: {c(C.WHITE, str(vkid))}  Найдено: {found_s}  {c(C.GRAY, f'~{rate:.1f} ID/с')}  ETA: {c(C.GRAY, eta)}  ", end="", flush=True)
            if vkid == sess["vk_user_id"]: continue
            profile = api_profile(vkid)
            if not profile: scanner["total_errors"] += 1; time.sleep(delay); continue
            u = profile.get("user", profile)
            if not u.get("first_name", ""): scanner["total_empty"] += 1; time.sleep(delay); continue
            name = f"{u.get('first_name','')} {u.get('last_name','')}".strip()
            cost = u.get("cost", u.get("sale_price", 0)); owner_raw = u.get("owner") or u.get("master_id")
            if scanner["skip_owned"] and is_my_slave(owner_raw): scanner["total_skipped"] += 1; time.sleep(delay); continue
            if not isinstance(cost, (int, float)) or cost <= 0: scanner["total_skipped"] += 1; time.sleep(delay); continue
            if cost > max_price: time.sleep(delay); continue
            shield = u.get("shield"); fetters = u.get("fetters"); group = u.get("group") or u.get("squad") or u.get("clan")
            group_name = group.get("name", "") if isinstance(group, dict) else None
            if scanner["skip_shielded"] and shield:
                try:
                    if datetime.fromisoformat(shield.replace("Z","")) > datetime.now(): scanner["total_skipped"] += 1; time.sleep(delay); continue
                except Exception: pass
            if scanner["skip_clans"] and group_name: scanner["total_skipped"] += 1; time.sleep(delay); continue
            scanner["total_found"] += 1
            entry = {"vkid": vkid, "name": name, "cost": cost, "owner_raw": owner_raw, "owner_str": get_owner_str(owner_raw),
                     "job": u.get("job","—"), "income": u.get("income", u.get("salary",0)), "slaves_count": u.get("slaves_count",0),
                     "shield": shield, "fetters": fetters, "group_name": group_name}
            scanner["found_list"].append(entry)
            print("\r" + " " * 120 + "\r", end="", flush=True)
            _scanner_display_found(entry, scanner["total_found"], -1)
            should_buy = False
            if auto_buy: should_buy = True; print(f"\n  {c(C.GREEN, '⚡ Авто-покупка...')}")
            else:
                print()
                ans = input(f"  {c(C.CYAN, '❯')} Купить {c(C.WHITE, name)} за {c(C.YELLOW, format_num(cost))}? {c(C.GRAY, '(y/n/stop)')}: ").strip().lower()
                if ans == "y": should_buy = True
                elif ans == "stop": log("Остановлено", "warn"); scanner["running"] = False; break
                else: log(f"Пропущен: {name}", "info")
            if should_buy:
                buy_result = api_buy(vkid, cost)
                if buy_result:
                    err_msg = buy_result.get("error") or buy_result.get("message")
                    if err_msg: log(f"Ошибка: {err_msg}", "err"); scanner["total_errors"] += 1; stats["errors"] += 1
                    else:
                        scanner["total_bought"] += 1; scanner["total_spent"] += cost
                        stats["bought_count"] += 1; stats["bought_total"] += cost
                        log(f"✓ Куплен: {name} за {format_num(cost)} 🪙  (баланс: {format_num(buy_result.get('new_balance','?'))})", "ok")
                else: log(f"Нет ответа", "err"); scanner["total_errors"] += 1
            print(); time.sleep(delay)
    except KeyboardInterrupt: print(); log("Прервано (Ctrl+C)", "warn")
    scanner["running"] = False
    print(); section("Итоги"); row("Просканировано", c(C.WHITE, format_num(scanner["total_scanned"]))); row("Найдено", c(C.GREEN if scanner["total_found"] > 0 else C.GRAY, format_num(scanner["total_found"]))); row("Куплено", c(C.CYAN, format_num(scanner["total_bought"]))); row("Потрачено", c(C.YELLOW, format_num(scanner["total_spent"])) + " 🪙"); row("Ошибок", c(C.RED if scanner["total_errors"] else C.GRAY, format_num(scanner["total_errors"]))); close()

def screen_scanner():
    while True:
        clear(); header("СКАНЕР VK ID  🔍"); print()
        if not sess["user_token"]: log("Нет токена — введи x-init-data через M", "err"); input(c(C.GRAY, "\n  Enter для возврата...")); return
        section("Настройки сканера")
        row("1  ID от",            c(C.WHITE, format_num(scanner["id_from"])))
        row("2  ID до",            c(C.WHITE, format_num(scanner["id_to"])))
        row("   Диапазон",         c(C.GRAY, format_num(max(0, scanner["id_to"]-scanner["id_from"]+1)) + " ID"))
        divider()
        row("3  Макс. цена",       c(C.YELLOW + C.BOLD, format_num(scanner["max_price"])) + " 🪙")
        row("4  Задержка",         c(C.GRAY, f"{scanner['delay']} сек"))
        row("5  Авто-покупка",     c(C.GREEN, "ДА") if scanner["auto_buy"] else c(C.GRAY, "нет"))
        divider()
        row("6  Пропуск щитов",    c(C.GREEN if scanner["skip_shielded"] else C.GRAY, "вкл" if scanner["skip_shielded"] else "выкл"))
        row("7  Пропуск кланов",   c(C.GREEN if scanner["skip_clans"] else C.GRAY, "вкл" if scanner["skip_clans"] else "выкл"))
        row("8  Пропуск своих",    c(C.GREEN if scanner["skip_owned"] else C.GRAY, "вкл" if scanner["skip_owned"] else "выкл"))
        close()
        if scanner["total_scanned"] > 0:
            print(); section("Последний скан")
            row("Просканировано", c(C.WHITE, format_num(scanner["total_scanned"]))); row("Найдено", c(C.GREEN if scanner["total_found"] > 0 else C.GRAY, format_num(scanner["total_found"]))); row("Куплено", c(C.CYAN, format_num(scanner["total_bought"]))); row("Потрачено", c(C.YELLOW, format_num(scanner["total_spent"])) + " 🪙"); close()
        print(); print(f"  {c(C.GREEN + C.BOLD, ' R ')}  {c(C.WHITE, '🚀 ЗАПУСТИТЬ')}")
        if scanner["found_list"]:
            _fc = len(scanner["found_list"])
            print(f"  {chr(27)}[93;1m F {chr(27)}[0m  {chr(27)}[97mНайденные ({_fc} шт.){chr(27)}[0m")
        print(f"  {c(C.RED + C.BOLD, ' Z ')}  {c(C.GRAY, 'Сбросить статистику')}"); print(f"  {c(C.GRAY, ' 0 ')}  {c(C.GRAY, 'Назад')}")
        choice = prompt("Действие или номер настройки")
        if choice == "0": return
        elif choice == "R": _scanner_run(); input(c(C.GRAY, "\n  Enter для возврата в меню..."))
        elif choice == "F":
            if scanner["found_list"]: _screen_scanner_found()
        elif choice == "Z": _scanner_reset_stats(); scanner["last_scanned"] = 0; log("Сброшено", "ok")
        elif choice == "1":
            v = input(f"  {c(C.GRAY, 'ID от')}: ").strip().replace(" ","")
            if v.isdigit() and int(v) > 0: scanner["id_from"] = int(v)
        elif choice == "2":
            v = input(f"  {c(C.GRAY, 'ID до')}: ").strip().replace(" ","")
            if v.isdigit() and int(v) > 0: scanner["id_to"] = int(v)
        elif choice == "3":
            v = input(f"  {c(C.GRAY, 'Макс. цена')}: ").strip().replace(" ","")
            if v.isdigit(): scanner["max_price"] = int(v); log(f"Макс. цена: {format_num(scanner['max_price'])}", "ok")
        elif choice == "4":
            v = input(f"  {c(C.GRAY, 'Задержка (мин 0.3)')}: ").strip()
            try:
                val = float(v)
                if val >= 0.3: scanner["delay"] = val; log(f"Задержка: {val} сек", "ok")
                else: log("Минимум 0.3 сек", "warn")
            except ValueError: log("Неверное число", "err")
        elif choice == "5":
            scanner["auto_buy"] = not scanner["auto_buy"]
            if scanner["auto_buy"]:
                print(f"\n  {c(C.YELLOW, '⚠  Бот будет покупать без подтверждения!')}")
                ans = input(f"  {c(C.GRAY, 'Уверен? (y/n)')}: ").strip().lower()
                if ans != "y": scanner["auto_buy"] = False
            log(f"Авто-покупка: {'ВКЛ' if scanner['auto_buy'] else 'выкл'}", "ok")
        elif choice == "6": scanner["skip_shielded"] = not scanner["skip_shielded"]
        elif choice == "7": scanner["skip_clans"] = not scanner["skip_clans"]
        elif choice == "8": scanner["skip_owned"] = not scanner["skip_owned"]

def _screen_scanner_found():
    while True:
        clear(); header(f"НАЙДЕННЫЕ  ({len(scanner['found_list'])} шт.)"); print()
        if not scanner["found_list"]: print(f"  {c(C.GRAY, 'Список пуст')}"); input(c(C.GRAY, "\n  Enter...")); return
        sorted_found = sorted(scanner["found_list"], key=lambda x: x.get("cost", 0))
        print(f"  {c(C.CYAN, '┌' + '─' * (W - 2))}")
        print(f"  {c(C.CYAN, '│')}  {c(C.GRAY, '  #')}  {c(C.GRAY, '      VK ID')}  {c(C.GRAY, 'Имя' + ' '*17)}  {c(C.GRAY, '      Цена')}")
        print(f"  {c(C.CYAN, '├' + '─' * (W - 2))}")
        for i, entry in enumerate(sorted_found, 1):
            mine = is_my_slave(entry.get("owner_raw")); status = c(C.CYAN, " ⛓") if mine else ""
            price_c = C.GREEN if entry["cost"] < 10_000 else (C.YELLOW if entry["cost"] < 50_000 else C.WHITE)
            _vid = entry["vkid"]; _cst = format_num(entry["cost"]); _nm = entry["name"][:20]
            print(f"  {c(C.CYAN, '│')}  {c(C.WHITE, f'{i:>3}')}  {c(C.GRAY, f'{_vid:>11}')}  {_nm:<20}  {c(price_c, f'{_cst:>10}')}{status}")
        print(f"  {c(C.CYAN, '└' + '─' * (W - 2))}")
        print(f"\n  Сумма: {c(C.YELLOW + C.BOLD, format_num(sum(e['cost'] for e in sorted_found)))} 🪙")
        print(); print(f"  {c(C.GRAY, 'Номер для просмотра, A для покупки всех, 0 для выхода')}")
        choice = prompt("Номер или действие")
        if choice == "0": return
        elif choice == "A":
            not_mine = [e for e in sorted_found if not is_my_slave(e.get("owner_raw"))]
            if not not_mine: log("Все уже твои!", "info"); input(c(C.GRAY, "\n  Enter...")); continue
            total = sum(e["cost"] for e in not_mine)
            print(f"\n  Покупка {c(C.WHITE, str(len(not_mine)))} рабов за {c(C.YELLOW, format_num(total))} 🪙")
            ans = input(f"  {c(C.RED, '❯')} {c(C.GRAY, 'Подтвердить? (yes)')}: ").strip().lower()
            if ans != "yes": continue
            print()
            for j, entry in enumerate(not_mine, 1):
                print(f"  {c(C.GRAY, f'[{j}/{len(not_mine)}]')} Покупаю {c(C.WHITE, entry['name'])} за {c(C.YELLOW, format_num(entry['cost']))}...", end=" ", flush=True)
                result = api_buy(entry["vkid"], entry["cost"])
                if result:
                    err_msg = result.get("error") or result.get("message")
                    if err_msg: print(c(C.RED, f"✗ {err_msg}")); stats["errors"] += 1
                    else: print(c(C.GREEN, "✓")); stats["bought_count"] += 1; stats["bought_total"] += entry["cost"]; scanner["total_bought"] += 1; scanner["total_spent"] += entry["cost"]; entry["_bought"] = True
                else: print(c(C.RED, "✗ нет ответа"))
                time.sleep(cfg["delay_buy"])
            input(c(C.GRAY, "\n  Enter..."))
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(sorted_found):
                entry = sorted_found[idx]; clear(); header(f"РАБА: {entry['name']}"); print()
                _scanner_display_found(entry, idx + 1, len(sorted_found))
                if not is_my_slave(entry.get("owner_raw")):
                    print()
                    ans = input(f"  {c(C.CYAN, '❯')} Купить за {c(C.YELLOW, format_num(entry['cost']))}? {c(C.GRAY, '(y/n)')}: ").strip().lower()
                    if ans == "y":
                        result = api_buy(entry["vkid"], entry["cost"])
                        if result:
                            err_msg = result.get("error") or result.get("message")
                            if err_msg: log(err_msg, "err")
                            else: log(f"Куплен: {entry['name']}!", "ok"); stats["bought_count"] += 1; stats["bought_total"] += entry["cost"]; entry["_bought"] = True
                        else: log("Нет ответа", "err")
                else: print(f"\n  {c(C.CYAN, '⛓  Уже твой раб')}")
                input(c(C.GRAY, "\n  Enter..."))


# ══════════════════════════════════════════════════════
#  ПАРСЕР ДЕШЁВЫХ ИГРОКОВ  💰
#  Сканирует диапазон VK ID, собирает ТОП-N самых
#  дешёвых игроков (их собственная цена как раба)
# ══════════════════════════════════════════════════════

cheap = {
    # Настройки
    "id_from":       1,
    "id_to":         10_000,
    "top_size":      50,          # сколько хранить в топе
    "delay":         0.5,
    "skip_shielded": True,
    "skip_clans":    False,
    "skip_owned":    True,        # пропускать уже моих рабов
    "free_only":     False,       # только свободные (без хозяина)
    "min_income":    0,           # минимальный доход /ч
    "sort_by":       "cost",      # cost | income | payback
    "workers":       10,          # число параллельных потоков
    # Состояние
    "running":       False,
    "last_scanned":  0,
    # Статистика
    "total_scanned": 0,
    "total_empty":   0,
    "total_skipped": 0,
    "total_errors":  0,
    # Результаты — список всех найденных игроков (не только топ)
    # сортируется по cheap["sort_by"] при отображении
    "results":       [],          # [{vkid, name, cost, income, payback, owner_raw, ...}]
}


def _cheap_reset():
    cheap["total_scanned"] = cheap["total_empty"] = cheap["total_skipped"] = cheap["total_errors"] = 0
    cheap["results"] = []


def _cheap_payback(cost, income):
    """Окупаемость в часах. 0 если нельзя посчитать."""
    if not income or income <= 0 or not cost or cost <= 0:
        return 0
    return cost / income


def _cheap_sort_key(entry):
    by = cheap["sort_by"]
    if by == "income":
        return -(entry.get("income") or 0)   # больше доход — выше
    elif by == "payback":
        pb = entry.get("payback", 0)
        return pb if pb > 0 else 999_999_999  # быстрее окупается — выше
    else:
        return entry.get("cost", 0)           # дешевле — выше


def _cheap_payback_str(pb):
    """Форматированная строка окупаемости."""
    if pb <= 0:
        return c(C.GRAY, "—")
    if pb < 1:
        return c(C.GREEN + C.BOLD, f"{pb*60:.0f} мин")
    elif pb < 24:
        return c(C.GREEN, f"{pb:.1f} ч")
    elif pb < 168:
        return c(C.YELLOW, f"{pb/24:.1f} дн")
    else:
        return c(C.RED, f"{pb/24:.0f} дн")


# ══════════════════════════════════════════════════════
#  ПАРСЕР ДЕШЁВЫХ — МНОГОПОТОЧНЫЙ _cheap_run
#  Параллельные HTTP-запросы через ThreadPoolExecutor
#  Результаты → очередь → главный поток (UI/покупка)
# ══════════════════════════════════════════════════════

def _cheap_run():
    """
    Многопоточный парсер дешёвых игроков.
    workers потоков шлют запросы параллельно,
    результаты кладут в очередь, главный поток
    обрабатывает UI и покупки.
    """
    import queue as _queue
    from concurrent.futures import ThreadPoolExecutor, as_completed

    id_from     = cheap["id_from"]
    id_to       = cheap["id_to"]
    total_range = id_to - id_from + 1

    if total_range <= 0:
        log("Неверный диапазон: id_from >= id_to", "err")
        return

    # ── Продолжение с последнего ID? ──
    resume_from = id_from
    if cheap["last_scanned"] > 0 and id_from <= cheap["last_scanned"] < id_to:
        print()
        print(f"  {c(C.YELLOW, '!')} Последний просканированный ID: "
              f"{c(C.WHITE + C.BOLD, str(cheap['last_scanned']))}")
        ans = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, 'Продолжить с него? (y/n)')}: ").strip().lower()
        if ans == "y":
            resume_from = cheap["last_scanned"] + 1
            total_range = id_to - resume_from + 1
            log(f"Продолжаю с ID {format_num(resume_from)}", "info")
        else:
            _cheap_reset()
            cheap["last_scanned"] = 0
    elif cheap["total_scanned"] == 0:
        _cheap_reset()

    workers    = cheap.get("workers", 10)
    blacklist  = cheap.get("blacklist", set())

    # ── Шапка запуска ──
    bar_width = 50
    print()
    print(f"  {c(C.YELLOW, '╔' + '═' * bar_width + '╗')}")
    print(f"  {c(C.YELLOW, '║')}  {c(C.BOLD + C.GREEN, '💰  ПАРСЕР ДЕШЁВЫХ ИГРОКОВ  [TURBO]')}"
          + " " * (bar_width - 38) + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '║')}" + " " * (bar_width + 2) + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '║')}  Диапазон:    "
          f"{c(C.WHITE, f'{format_num(resume_from)} → {format_num(id_to)}')}  "
          f"({c(C.GRAY, format_num(total_range) + ' ID')})" + " " * 5 + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '║')}  Потоков:     {c(C.CYAN + C.BOLD, str(workers))}"
          + " " * (bar_width - 18) + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '║')}  Макс. цена:  {c(C.YELLOW, format_num(cheap['max_price']) + ' 🪙')}"
          + " " * max(0, bar_width - 28) + c(C.YELLOW, "║"))
    flags = []
    if cheap["skip_shielded"]:  flags.append("щиты")
    if cheap["skip_clans"]:     flags.append("кланы")
    if cheap["skip_owned"]:     flags.append("свои рабы")
    if cheap["free_only"]:      flags.append("только свободных")
    if cheap["min_income"] > 0: flags.append(f"доход≥{format_num(cheap['min_income'])}")
    flags_s = ", ".join(flags) if flags else "нет"
    print(f"  {c(C.YELLOW, '║')}  Фильтры:     {c(C.GRAY, flags_s)}"
          + " " * max(0, bar_width - 14 - len(flags_s)) + c(C.YELLOW, "║"))
    auto_s = c(C.GREEN, "ДА (без подтверждения)") if cheap["auto_buy"] else c(C.GRAY, "нет — спрашивать")
    print(f"  {c(C.YELLOW, '║')}  Авто-покупка: {auto_s}" + " " * 6 + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '║')}" + " " * (bar_width + 2) + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '║')}  {c(C.GRAY, 'Ctrl+C — стоп   |   n/Enter — пропустить   |   q — выход')}"
          + " " * 2 + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '╚' + '═' * bar_width + '╝')}")
    print()

    # ── Общее состояние (thread-safe через Lock) ──
    _lock       = threading.Lock()
    result_q    = _queue.Queue()   # {"type": "found"|"stat", ...}
    cheap["running"] = True
    start_time  = time.time()
    bought_now  = 0

    # ── Воркер: запрашивает один профиль, кладёт результат в очередь ──
    def _worker(vkid):
        if not cheap["running"]:
            return
        if vkid == sess["vk_user_id"] or vkid in blacklist:
            result_q.put({"type": "stat", "stat": "skipped"})
            return

        profile = api_profile(vkid)
        if not profile:
            result_q.put({"type": "stat", "stat": "error", "vkid": vkid})
            return

        u = profile.get("user", profile)
        if not u.get("first_name", ""):
            result_q.put({"type": "stat", "stat": "empty"})
            return

        name       = f"{u.get('first_name','').strip()} {u.get('last_name','').strip()}".strip()
        cost       = u.get("cost", u.get("sale_price", 0))
        owner_raw  = u.get("owner") or u.get("master_id")
        income     = u.get("income", u.get("salary", 0)) or 0
        job        = u.get("job", "—")
        shield     = u.get("shield")
        fetters    = u.get("fetters")
        group      = u.get("group") or u.get("squad") or u.get("clan")
        group_name = group.get("name", "") if isinstance(group, dict) else None
        slaves_cnt = u.get("slaves_count", 0)

        # Фильтры
        if not isinstance(cost, (int, float)) or cost <= 0:
            result_q.put({"type": "stat", "stat": "skipped"}); return
        if cost > cheap["max_price"]:
            result_q.put({"type": "stat", "stat": "none"}); return
        if cheap["skip_owned"] and is_my_slave(owner_raw):
            result_q.put({"type": "stat", "stat": "skipped"}); return
        if cheap["free_only"] and get_owner_id(owner_raw) is not None:
            result_q.put({"type": "stat", "stat": "skipped"}); return
        if cheap["skip_shielded"] and shield:
            try:
                if datetime.fromisoformat(shield.replace("Z", "")) > datetime.now():
                    result_q.put({"type": "stat", "stat": "skipped"}); return
            except Exception:
                pass
        if cheap["skip_clans"] and group_name:
            result_q.put({"type": "stat", "stat": "skipped"}); return
        if cheap["min_income"] > 0 and income < cheap["min_income"]:
            result_q.put({"type": "stat", "stat": "skipped"}); return

        # Нашли!
        payback = _cheap_payback(cost, income)
        entry = {
            "vkid":         vkid,
            "name":         name,
            "cost":         int(cost),
            "income":       int(income),
            "payback":      payback,
            "owner_raw":    owner_raw,
            "owner_str":    get_owner_str(owner_raw),
            "job":          job,
            "shield":       shield,
            "fetters":      fetters,
            "group_name":   group_name,
            "slaves_count": slaves_cnt,
            "_bought":      False,
            "_scanned_at":  datetime.now().strftime("%H:%M:%S"),
        }
        result_q.put({"type": "found", "entry": entry})

    # ── Главный цикл: submit задач + drain очереди ──
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            # Скармливаем все ID пулу
            futures = {}
            for vkid in range(resume_from, id_to + 1):
                if not cheap["running"]:
                    break
                fut = pool.submit(_worker, vkid)
                futures[fut] = vkid

            total_submitted = len(futures)
            processed       = 0

            for fut in as_completed(futures):
                if not cheap["running"]:
                    # Отменяем оставшиеся (не начатые)
                    for f in futures:
                        f.cancel()
                    break

                processed += 1
                vkid_done  = futures[fut]
                elapsed    = time.time() - start_time

                # Обновляем last_scanned (потоки завершаются не по порядку)
                with _lock:
                    if vkid_done > cheap["last_scanned"]:
                        cheap["last_scanned"] = vkid_done
                    cheap["total_scanned"] += 1

                # Drain очереди результатов
                while not result_q.empty():
                    msg = result_q.get_nowait()
                    if msg["type"] == "stat":
                        s = msg["stat"]
                        if s == "error":   cheap["total_errors"]  += 1; stats["errors"] += 1
                        elif s == "empty": cheap["total_empty"]   += 1
                        elif s == "skipped": cheap["total_skipped"] += 1
                        # "none" = выше макс. цены, не считаем
                    elif msg["type"] == "found":
                        entry = msg["entry"]
                        with _lock:
                            existing_idx = next(
                                (i for i, e in enumerate(cheap["results"]) if e["vkid"] == entry["vkid"]), None
                            )
                            if existing_idx is not None:
                                cheap["results"][existing_idx] = entry
                            else:
                                cheap["results"].append(entry)
                            if len(cheap["results"]) > cheap["top_size"] * 4:
                                cheap["results"].sort(key=_cheap_sort_key)
                                cheap["results"] = cheap["results"][: cheap["top_size"] * 2]

                        # ── Показываем находку ──
                        print("\r" + " " * 130 + "\r", end="", flush=True)
                        cost   = entry["cost"]
                        income = entry["income"]
                        payback = entry["payback"]
                        name   = entry["name"]
                        owner_raw = entry["owner_raw"]
                        group_name = entry["group_name"]

                        if cost < 5_000:     price_c = C.GREEN + C.BOLD
                        elif cost < 20_000:  price_c = C.GREEN
                        elif cost < 100_000: price_c = C.YELLOW
                        else:                price_c = C.WHITE

                        payback_s  = _cheap_payback_str(payback) if income > 0 else c(C.GRAY, "—")
                        owner_flag = c(C.CYAN, "⛓ ТВОЙ  ") if is_my_slave(owner_raw) else ""
                        clan_flag  = c(C.PURPLE, f"[{group_name[:10]}] ") if group_name else ""

                        print(
                            f"  {c(C.GREEN + C.BOLD, '★')}  "
                            f"{c(C.WHITE + C.BOLD, name[:22]):<22}  "
                            f"{c(price_c, format_num(cost) + ' 🪙'):<14}  "
                            f"{c(C.GRAY, f'доход:{format_num(income)}/ч'):<18}  "
                            f"окуп:{payback_s:<10}  "
                            f"{owner_flag}{clan_flag}"
                            f"{c(C.GRAY, 'id:' + str(entry['vkid']))}"
                        )

                        # Авто-покупка или запрос
                        should_buy = cheap["auto_buy"] and not is_my_slave(owner_raw)
                        if not cheap["auto_buy"] and not is_my_slave(owner_raw):
                            try:
                                ans = input(
                                    f"     {c(C.CYAN, '❯')} "
                                    f"Купить за {c(C.YELLOW + C.BOLD, format_num(cost))} 🪙? "
                                    f"{c(C.GRAY, '(y=да / n=нет / q=стоп)')}: "
                                ).strip().lower()
                            except (KeyboardInterrupt, EOFError):
                                ans = "q"
                            if ans == "q":
                                log("Скан остановлен пользователем", "warn")
                                cheap["running"] = False
                                break
                            elif ans == "y":
                                should_buy = True
                        elif is_my_slave(owner_raw):
                            print(f"     {c(C.CYAN, '⛓  уже твой раб — пропускаем')}")

                        if should_buy:
                            buy_result = api_buy(entry["vkid"], cost)
                            if buy_result:
                                err_msg = buy_result.get("error") or buy_result.get("message")
                                if err_msg:
                                    print(f"     {c(C.RED, f'✗  {err_msg}')}")
                                    cheap["total_errors"] += 1
                                    stats["errors"] += 1
                                else:
                                    entry["_bought"] = True
                                    bought_now += 1
                                    cheap["total_bought"] = cheap.get("total_bought", 0) + 1
                                    cheap["total_spent"]  = cheap.get("total_spent", 0) + cost
                                    stats["bought_count"] += 1
                                    stats["bought_total"] += cost
                                    new_bal = buy_result.get("new_balance", "?")
                                    print(
                                        f"     {c(C.GREEN + C.BOLD, '✓ КУПЛЕН!')}  "
                                        f"потрачено: {c(C.YELLOW, format_num(cost))} 🪙  "
                                        f"баланс: {c(C.CYAN, format_num(new_bal))} 🪙"
                                    )
                            else:
                                print(f"     {c(C.RED, '✗  нет ответа от сервера')}")
                                cheap["total_errors"] += 1

                # ── Прогресс каждые N обработанных ──
                if processed % max(1, workers) == 0 or processed == total_submitted:
                    elapsed = time.time() - start_time
                    pct     = processed / total_submitted if total_submitted > 0 else 0
                    filled  = int(32 * pct)
                    bar     = c(C.GREEN, "█" * filled) + c(C.GRAY, "░" * (32 - filled))
                    rate    = processed / elapsed if elapsed > 0 else 0
                    eta_s   = _scanner_eta(processed, total_submitted, elapsed)
                    top_n   = len(cheap["results"])
                    min_c   = ""
                    if cheap["results"]:
                        top1  = sorted(cheap["results"], key=_cheap_sort_key)[0]
                        min_c = f"  мин:{c(C.YELLOW, format_num(top1['cost']))}"
                    print(
                        f"\r  [{bar}] {c(C.WHITE + C.BOLD, f'{pct*100:4.1f}%')}  "
                        f"ID:{c(C.WHITE, str(vkid_done))}  "
                        f"Найдено:{c(C.GREEN + C.BOLD, str(top_n))}  "
                        f"Куплено:{c(C.CYAN, str(bought_now))}  "
                        f"{c(C.CYAN + C.BOLD, f'{rate:.1f}/с')} [{workers}T]"
                        f"{min_c}  "
                        f"ETA:{c(C.GRAY, eta_s)}  ",
                        end="", flush=True,
                    )

    except KeyboardInterrupt:
        print()
        log("Сканирование прервано (Ctrl+C)", "warn")
        cheap["running"] = False

    cheap["running"] = False
    elapsed_total = time.time() - start_time

    # Финальная сортировка и обрезка
    cheap["results"].sort(key=_cheap_sort_key)
    cheap["results"] = cheap["results"][: cheap["top_size"]]

    # ── Итоговый отчёт ──
    mins   = int(elapsed_total // 60)
    secs   = int(elapsed_total % 60)
    time_s = f"{mins}м {secs}с" if mins > 0 else f"{secs}с"
    rate   = cheap["total_scanned"] / elapsed_total if elapsed_total > 0 else 0

    print("\r" + " " * 130 + "\r")
    print()
    print(f"  {c(C.YELLOW, '╔══════════════════════════════════════════════════╗')}")
    print(f"  {c(C.YELLOW, '║')}  {c(C.BOLD + C.WHITE, '📊  ИТОГИ ПАРСИНГА')}"
          + " " * 34 + c(C.YELLOW, "║"))
    print(f"  {c(C.YELLOW, '╚══════════════════════════════════════════════════╝')}")
    print()

    section("Статистика сканирования")
    row("Просканировано",     c(C.WHITE,  format_num(cheap["total_scanned"])))
    row("Не в игре",          c(C.GRAY,   format_num(cheap["total_empty"])))
    row("Пропущено фильтром", c(C.GRAY,   format_num(cheap["total_skipped"])))
    row("Ошибок запросов",    c(C.RED if cheap["total_errors"] else C.GRAY,
                               format_num(cheap["total_errors"])))
    row("Потоков",            c(C.CYAN,   str(workers)))
    row("Скорость",           c(C.CYAN + C.BOLD, f"{rate:.1f} ID/сек"))
    row("Время",              c(C.GRAY,   time_s))
    divider()
    found_n = len(cheap["results"])
    row("Найдено дешёвых",    c(C.GREEN + C.BOLD if found_n > 0 else C.GRAY,
                               format_num(found_n)))
    row("Куплено сейчас",     c(C.CYAN,   format_num(bought_now)))
    row("Потрачено",          c(C.YELLOW, format_num(cheap.get("total_spent", 0))) + " 🪙")
    row("Диапазон завершён",  c(C.GRAY,
        f"{format_num(resume_from)} → {format_num(cheap['last_scanned'])}"))
    close()

    if cheap["results"]:
        print()
        _cheap_display_top()


# ══════════════════════════════════════════════════════
#  ПАРСЕР ДЕШЁВЫХ — ОТОБРАЖЕНИЕ ТОПА
# ══════════════════════════════════════════════════════

def _cheap_display_top(max_rows=None):
    """Таблица топа дешёвых игроков в терминале."""
    results = sorted(cheap["results"], key=_cheap_sort_key)
    if max_rows:
        results = results[:max_rows]
    if not results:
        print(f"  {c(C.GRAY, 'Список пуст')}")
        return

    _sort_lbl = cheap["sort_by"]
    print()
    print(f"  {c(C.YELLOW, '┌─')} {c(C.BOLD + C.WHITE, f'ТОП ДЕШЁВЫХ ({len(results)} шт.)')}  "
          f"{c(C.GRAY, f'сортировка: {_sort_lbl}')}")
    print(f"  {c(C.YELLOW, '│')}")
    print(f"  {c(C.YELLOW, '│')}  "
          f"{c(C.GRAY, '  #')}  "
          f"{c(C.GRAY, '      VK ID')}  "
          f"{c(C.GRAY, 'Имя' + ' ' * 17)}  "
          f"{c(C.GRAY, '       Цена')}  "
          f"{c(C.GRAY, '    Доход/ч')}  "
          f"{c(C.GRAY, 'Окупаемость')}")
    print(f"  {c(C.YELLOW, '├' + '─' * (W - 2))}")

    for i, e in enumerate(results, 1):
        cost    = e["cost"]
        income  = e.get("income", 0)
        payback = e.get("payback", 0)
        nm      = e["name"][:20]
        vid     = e["vkid"]
        bought  = e.get("_bought", False)
        mine    = is_my_slave(e.get("owner_raw"))

        if cost < 5_000:     pc = C.GREEN + C.BOLD
        elif cost < 20_000:  pc = C.GREEN
        elif cost < 100_000: pc = C.YELLOW
        else:                pc = C.WHITE

        mark = c(C.GREEN, "✓") if bought else (c(C.CYAN, "⛓") if mine else " ")

        print(
            f"  {c(C.YELLOW, '│')} {mark} "
            f"{c(C.GRAY, f'{i:>3}')}  "
            f"{c(C.GRAY, f'{vid:>11}')}  "
            f"{nm:<20}  "
            f"{c(pc, f'{format_num(cost):>11}')}  "
            f"{c(C.GRAY, (format_num(income) + '/ч').rjust(10) if income else '—'.rjust(10))}  "
            f"{_cheap_payback_str(payback)}"
        )

    print(f"  {c(C.YELLOW, '└' + '─' * (W - 2))}")

    total_not_mine = sum(e["cost"] for e in results if not is_my_slave(e.get("owner_raw")) and not e.get("_bought"))
    total_all      = sum(e["cost"] for e in results)
    print(f"\n  Сумма топа: {c(C.YELLOW + C.BOLD, format_num(total_all))} 🪙  │  "
          f"Доступно для покупки: {c(C.GREEN, format_num(total_not_mine))} 🪙")


# ══════════════════════════════════════════════════════
#  ПАРСЕР ДЕШЁВЫХ — ЭКРАН ПРОСМОТРА РЕЗУЛЬТАТОВ
# ══════════════════════════════════════════════════════

def _cheap_screen_results():
    """Просмотр найденных игроков с покупкой по одному или всех."""
    while True:
        results = sorted(cheap["results"], key=_cheap_sort_key)

        clear()
        header(f"ДЕШЁВЫЕ ИГРОКИ  ({len(results)} шт.)")
        print()

        if not results:
            print(f"  {c(C.GRAY, 'Список пуст — сначала запусти парсер (R)')}")
            input(c(C.GRAY, "\n  Enter для возврата...")); return

        _cheap_display_top()

        not_mine = [e for e in results if not is_my_slave(e.get("owner_raw")) and not e.get("_bought")]
        print()
        print(f"  {c(C.GRAY, 'Действия:')}")
        print(f"  {c(C.WHITE, '  #')}  Просмотр/покупка по номеру")
        print(f"  {c(C.WHITE, '  A')}  Купить всех ({c(C.GREEN + C.BOLD, str(len(not_mine)))} доступных)")
        print(f"  {c(C.WHITE, '  E')}  Экспорт в CSV файл")
        print(f"  {c(C.WHITE, '  S')}  Сменить сортировку (сейчас: {c(C.YELLOW, cheap['sort_by'])})")
        print(f"  {c(C.WHITE, '  0')}  Назад")

        choice = prompt("Номер игрока или действие")

        if choice == "0":
            return

        elif choice == "S":
            sorts = ["cost", "income", "payback"]
            cur   = cheap["sort_by"]
            nxt   = sorts[(sorts.index(cur) + 1) % len(sorts)]
            cheap["sort_by"] = nxt
            labels = {"cost": "по цене (↑)", "income": "по доходу (↓)", "payback": "по окупаемости (↑)"}
            log(f"Сортировка: {labels[nxt]}", "ok")

        elif choice == "E":
            _cheap_export_csv()
            input(c(C.GRAY, "\n  Enter для продолжения..."))

        elif choice == "A":
            if not not_mine:
                log("Всех уже купил или список пуст", "info")
                input(c(C.GRAY, "\n  Enter...")); continue

            total_sum = sum(e["cost"] for e in not_mine)
            print(f"\n  Купить {c(C.WHITE + C.BOLD, str(len(not_mine)))} игроков "
                  f"на сумму {c(C.YELLOW + C.BOLD, format_num(total_sum))} 🪙")
            confirm = input(f"  {c(C.RED, '❯')} {c(C.GRAY, 'Подтвердить? (yes для подтверждения)')}: ").strip().lower()
            if confirm != "yes":
                log("Отменено", "info"); continue

            print()
            ok_count = 0
            for j, e in enumerate(not_mine, 1):
                pfx = c(C.GRAY, f"  [{j}/{len(not_mine)}]")
                print(f"{pfx}  {c(C.WHITE, e['name'][:22])}  "
                      f"{c(C.YELLOW, format_num(e['cost']))} 🪙...",
                      end=" ", flush=True)
                r = api_buy(e["vkid"], e["cost"])
                if r and not (r.get("error") or r.get("message")):
                    print(c(C.GREEN, "✓ куплен"))
                    e["_bought"]  = True
                    ok_count     += 1
                    cheap["total_bought"] = cheap.get("total_bought", 0) + 1
                    cheap["total_spent"]  = cheap.get("total_spent", 0) + e["cost"]
                    stats["bought_count"] += 1
                    stats["bought_total"] += e["cost"]
                else:
                    msg = (r.get("error") or r.get("message", "нет ответа")) if r else "нет ответа"
                    print(c(C.RED, f"✗ {msg}"))
                    stats["errors"] += 1
                time.sleep(cfg["delay_buy"])

            print()
            log(f"Куплено {ok_count} из {len(not_mine)}", "ok")
            input(c(C.GRAY, "\n  Enter для продолжения..."))

        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(results):
                _cheap_screen_single(results[idx])
            else:
                log(f"Нет игрока с номером {choice}", "warn")


def _cheap_screen_single(entry):
    """Подробный просмотр одного найденного игрока с возможностью купить."""
    clear()
    name   = entry["name"]
    vkid   = entry["vkid"]
    cost   = entry["cost"]
    income = entry.get("income", 0)

    header(f"{name[:40]}")
    print()

    section(c(C.WHITE + C.BOLD, name) + c(C.GRAY, f"  [vk.com/id{vkid}]"))
    divider()
    row("Цена",          c(C.YELLOW + C.BOLD, format_num(cost)) + " 🪙")
    row("Хозяин",        entry.get("owner_str", "—"))
    row("Доход/час",     c(C.GREEN, format_num(income)) + "/ч" if income else c(C.GRAY, "—"))
    row("Рабов",         c(C.WHITE, str(entry.get("slaves_count", 0))))
    row("Работа",        c(C.PURPLE, entry.get("job", "—")))
    if entry.get("group_name"):
        row("Клан",      c(C.CYAN, entry["group_name"]))
    if entry.get("shield"):
        row("Щит до",    c(C.BLUE, format_date(entry["shield"])))
    if entry.get("fetters"):
        row("Оковы до",  c(C.YELLOW, format_date(entry["fetters"])))
    row("Найден в",      c(C.GRAY, entry.get("_scanned_at", "—")))
    divider()

    # Окупаемость
    pb = entry.get("payback", 0)
    row("Окупаемость",   _cheap_payback_str(pb))
    if income > 0 and cost > 0:
        daily_income = income * 24
        weekly_income = income * 168
        row("Доход в день",  c(C.GREEN, format_num(daily_income)) + " 🪙")
        row("Доход в неделю",c(C.GREEN, format_num(weekly_income)) + " 🪙")
    close()

    already_mine   = is_my_slave(entry.get("owner_raw"))
    already_bought = entry.get("_bought", False)

    if already_mine:
        print(f"\n  {c(C.CYAN, '⛓  Это уже твой раб')}")
    elif already_bought:
        print(f"\n  {c(C.GREEN, '✓  Уже куплен в этой сессии')}")
    else:
        print()
        ans = input(
            f"  {c(C.CYAN, '❯')} Купить {c(C.WHITE, name[:20])} за "
            f"{c(C.YELLOW + C.BOLD, format_num(cost))} 🪙? "
            f"{c(C.GRAY, '(y/n)')}: "
        ).strip().lower()

        if ans == "y":
            r = api_buy(vkid, cost)
            print()
            if r and not (r.get("error") or r.get("message")):
                entry["_bought"] = True
                cheap["total_bought"] = cheap.get("total_bought", 0) + 1
                cheap["total_spent"]  = cheap.get("total_spent", 0) + cost
                stats["bought_count"] += 1
                stats["bought_total"] += cost
                nb = r.get("new_balance", "?")
                log(f"✓ Куплен: {name}!  баланс: {format_num(nb)} 🪙", "ok")
            else:
                msg = (r.get("error") or r.get("message", "")) if r else "нет ответа"
                log(msg, "err")
                stats["errors"] += 1

    input(c(C.GRAY, "\n  Enter для возврата..."))


# ══════════════════════════════════════════════════════
#  ПАРСЕР ДЕШЁВЫХ — ЭКСПОРТ В CSV
# ══════════════════════════════════════════════════════

def _cheap_export_csv():
    """Экспортирует текущие результаты в CSV файл."""
    if not cheap["results"]:
        log("Список пуст — нечего экспортировать", "warn")
        return

    import csv as _csv
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename  = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"slaves_cheap_{ts}.csv")

    fields = ["vkid", "name", "cost", "income", "payback", "job", "slaves_count",
              "group_name", "owner_str", "_bought", "_scanned_at"]

    try:
        with open(filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = _csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for e in sorted(cheap["results"], key=_cheap_sort_key):
                row_data = {k: e.get(k, "") for k in fields}
                row_data["payback"] = f"{e.get('payback', 0):.2f}"
                writer.writerow(row_data)
        log(f"Экспорт: {os.path.basename(filename)}  ({len(cheap['results'])} записей)", "ok")
        print(f"  {c(C.GRAY, 'Путь:')} {c(C.WHITE, filename)}")
    except Exception as ex:
        log(f"Ошибка экспорта: {ex}", "err")


# ══════════════════════════════════════════════════════
#  ЭКРАН — ПАРСЕР ДЕШЁВЫХ (главный экран настроек)
# ══════════════════════════════════════════════════════

def screen_cheapest():
    """Главный экран парсера дешёвых игроков."""
    while True:
        clear()
        header("ПАРСЕР ДЕШЁВЫХ ИГРОКОВ  💰")
        print()

        if not sess["user_token"]:
            log("Нет токена — введи x-init-data через M", "err")
            input(c(C.GRAY, "\n  Enter для возврата...")); return

        # Настройки
        section("Настройки")
        row("1  ID от",              c(C.WHITE, format_num(cheap["id_from"])))
        row("2  ID до",              c(C.WHITE, format_num(cheap["id_to"])))
        rng_size = max(0, cheap["id_to"] - cheap["id_from"] + 1)
        row("   Диапазон",           c(C.GRAY, f"{format_num(rng_size)} ID"))
        divider()
        row("3  Макс. цена",         c(C.YELLOW + C.BOLD, format_num(cheap["max_price"])) + " 🪙")
        row("4  Размер топа",        c(C.WHITE, str(cheap["top_size"])))
        row("5  Потоков (workers)",   c(C.CYAN + C.BOLD, str(cheap.get("workers", 10))) + c(C.GRAY, "  (ускорение парсера)"))
        row("6  Авто-покупка",       c(C.GREEN, "ДА — без подтверждения") if cheap["auto_buy"] else c(C.GRAY, "нет — спрашивать"))
        row("T  Сортировка",         c(C.YELLOW, cheap["sort_by"]))
        divider()
        row("7  Пропуск щитов",      c(C.GREEN if cheap["skip_shielded"] else C.GRAY,
                                      "вкл" if cheap["skip_shielded"] else "выкл"))
        row("8  Пропуск кланов",     c(C.GREEN if cheap["skip_clans"] else C.GRAY,
                                      "вкл" if cheap["skip_clans"] else "выкл"))
        row("9  Пропуск своих",      c(C.GREEN if cheap["skip_owned"] else C.GRAY,
                                      "вкл" if cheap["skip_owned"] else "выкл"))
        row("G  Только свободных",   c(C.GREEN if cheap["free_only"] else C.GRAY,
                                      "вкл" if cheap["free_only"] else "выкл"))
        row("I  Мин. доход/ч",       c(C.GRAY, format_num(cheap["min_income"]) + " (0 = любой)"))
        close()

        # Оценка времени
        if rng_size > 0:
            est_sec = rng_size * (cheap["delay"] + 0.25)
            est_min = est_sec / 60
            if est_min > 120:
                est_s = c(C.RED, f"~{est_min/60:.1f} ч")
            elif est_min > 30:
                est_s = c(C.YELLOW, f"~{est_min:.0f} мин")
            else:
                est_s = c(C.GRAY, f"~{est_min:.0f} мин")
            print(f"\n  {c(C.GRAY, 'Оценка времени:')} {est_s}")

        # Статистика последнего скана
        if cheap["total_scanned"] > 0:
            print()
            section("Последний скан")
            row("Просканировано", c(C.WHITE,  format_num(cheap["total_scanned"])))
            row("Найдено",        c(C.GREEN,  format_num(len(cheap["results"]))))
            row("Куплено",        c(C.CYAN,   format_num(cheap.get("total_bought", 0))))
            row("Потрачено",      c(C.YELLOW, format_num(cheap.get("total_spent", 0))) + " 🪙")
            if cheap["last_scanned"] > 0:
                row("Последний ID", c(C.GRAY, format_num(cheap["last_scanned"])))
            close()

        # Действия
        print()
        print(f"  {c(C.GREEN + C.BOLD, ' R ')}  {c(C.WHITE + C.BOLD, '🚀 ЗАПУСТИТЬ ПАРСЕР')}")
        if (cheap["last_scanned"] > 0 and
                cheap["id_from"] <= cheap["last_scanned"] < cheap["id_to"]):
            nxt_id = cheap["last_scanned"] + 1
            print(f"  {c(C.CYAN + C.BOLD, ' C ')}  {c(C.WHITE, f'Продолжить с ID {format_num(nxt_id)}')}")
        if cheap["results"]:
            found_cnt = len(cheap["results"])
            print(f"  {c(C.YELLOW + C.BOLD, ' F ')}  {c(C.WHITE, f'Просмотр результатов ({found_cnt} игроков)')}")
            print(f"  {c(C.WHITE, ' E ')}  {c(C.GRAY, 'Экспорт в CSV')}")
        print(f"  {c(C.RED + C.BOLD, ' Z ')}  {c(C.GRAY, 'Сбросить статистику и результаты')}")
        print(f"  {c(C.GRAY, ' 0 ')}  {c(C.GRAY, 'Назад в меню')}")

        ch = prompt("Действие или номер настройки")

        if ch == "0":
            return

        elif ch == "R":
            cheap["last_scanned"] = 0
            _cheap_reset()
            cheap.setdefault("total_bought", 0)
            cheap.setdefault("total_spent", 0)
            cheap.setdefault("blacklist", set())
            _cheap_run()
            input(c(C.GRAY, "\n  Enter для возврата в меню парсера..."))

        elif ch == "C":
            cheap.setdefault("total_bought", 0)
            cheap.setdefault("total_spent", 0)
            cheap.setdefault("blacklist", set())
            _cheap_run()
            input(c(C.GRAY, "\n  Enter для возврата в меню парсера..."))

        elif ch == "F":
            _cheap_screen_results()

        elif ch == "E":
            _cheap_export_csv()
            input(c(C.GRAY, "\n  Enter для продолжения..."))

        elif ch == "Z":
            _cheap_reset()
            cheap["last_scanned"]   = 0
            cheap["total_bought"]   = 0
            cheap["total_spent"]    = 0
            log("Статистика и результаты сброшены", "ok")

        elif ch == "T":
            sorts = ["cost", "income", "payback"]
            labels = {"cost": "по цене", "income": "по доходу", "payback": "по окупаемости"}
            nxt = sorts[(sorts.index(cheap["sort_by"]) + 1) % len(sorts)]
            cheap["sort_by"] = nxt
            log(f"Сортировка: {labels[nxt]}", "ok")

        elif ch == "1":
            v = input(f"  {c(C.GRAY, 'ID от')}: ").strip().replace(" ", "").replace("_", "")
            if v.isdigit() and int(v) > 0:
                cheap["id_from"] = int(v)
                log(f"ID от: {format_num(cheap['id_from'])}", "ok")

        elif ch == "2":
            v = input(f"  {c(C.GRAY, 'ID до')}: ").strip().replace(" ", "").replace("_", "")
            if v.isdigit() and int(v) > 0:
                cheap["id_to"] = int(v)
                log(f"ID до: {format_num(cheap['id_to'])}", "ok")

        elif ch == "3":
            v = input(f"  {c(C.GRAY, 'Макс. цена (купить только если дешевле)')}: ").strip().replace(" ", "").replace("_", "")
            if v.isdigit():
                cheap["max_price"] = int(v)
                log(f"Макс. цена: {format_num(cheap['max_price'])} 🪙", "ok")

        elif ch == "4":
            v = input(f"  {c(C.GRAY, 'Размер топа (сколько хранить)')}: ").strip()
            if v.isdigit() and int(v) > 0:
                cheap["top_size"] = int(v)
                log(f"Топ: {v} игроков", "ok")

        elif ch == "5":
            print(f"\n  {c(C.GRAY, 'Рекомендации: 5 — осторожно, 10 — оптимально, 20 — быстро, 50 — максимум')}")
            v = input(f"  {c(C.GRAY, 'Число потоков (1–50)')}: ").strip()
            if v.isdigit() and 1 <= int(v) <= 50:
                cheap["workers"] = int(v)
                log(f"Потоков: {v}", "ok")
            else:
                log("Введи число от 1 до 50", "err")

        elif ch == "6":
            cheap["auto_buy"] = not cheap["auto_buy"]
            if cheap["auto_buy"]:
                print(f"\n  {c(C.YELLOW + C.BOLD, '⚠  Авто-покупка: бот будет покупать БЕЗ подтверждения!')}")
                confirm = input(f"  {c(C.GRAY, 'Уверен? (y/n)')}: ").strip().lower()
                if confirm != "y":
                    cheap["auto_buy"] = False
                    log("Авто-покупка отключена", "info")
                else:
                    log("Авто-покупка ВКЛЮЧЕНА", "ok")
            else:
                log("Авто-покупка выключена — будет запрашивать", "ok")

        elif ch == "7":
            cheap["skip_shielded"] = not cheap["skip_shielded"]
            log(f"Пропуск щитов: {'вкл' if cheap['skip_shielded'] else 'выкл'}", "ok")

        elif ch == "8":
            cheap["skip_clans"] = not cheap["skip_clans"]
            log(f"Пропуск кланов: {'вкл' if cheap['skip_clans'] else 'выкл'}", "ok")

        elif ch == "9":
            cheap["skip_owned"] = not cheap["skip_owned"]
            log(f"Пропуск своих рабов: {'вкл' if cheap['skip_owned'] else 'выкл'}", "ok")

        elif ch == "G":
            cheap["free_only"] = not cheap["free_only"]
            if cheap["free_only"]:
                log("Только свободные (без хозяина)", "ok")
            else:
                log("Все игроки (свободные и несвободные)", "ok")

        elif ch == "I":
            v = input(f"  {c(C.GRAY, 'Минимальный доход/ч (0 = любой)')}: ").strip().replace(" ", "")
            if v.isdigit():
                cheap["min_income"] = int(v)
                log(f"Мин. доход: {format_num(cheap['min_income'])}/ч", "ok")


# ══════════════════════════════════════════════════════
#  МОНИТОРИНГ — следим за конкретными ID
#  Периодически проверяем цену и покупаем при падении
# ══════════════════════════════════════════════════════

monitor_cfg = {
    "targets":      [],      # список vkid для мониторинга
    "max_price":    50_000,
    "interval":     60,      # секунд между проверками
    "auto_buy":     True,
    "running":      False,
    "check_count":  0,
    "bought_count": 0,
    "prices":       {},      # vkid -> последняя известная цена
}


def _monitor_worker():
    """Фоновый поток мониторинга конкретных VK ID."""
    log("[мониторинг] Старт...", "info")
    while monitor_cfg["running"]:
        monitor_cfg["check_count"] += 1
        ts = datetime.now().strftime("%H:%M:%S")

        for vkid in list(monitor_cfg["targets"]):
            if not monitor_cfg["running"]:
                break
            if vkid == sess["vk_user_id"]:
                continue

            profile = api_profile(vkid)
            if not profile:
                continue

            u      = profile.get("user", profile)
            name   = f"{u.get('first_name','')} {u.get('last_name','')}".strip() or str(vkid)
            cost   = u.get("cost", 0)
            owner  = u.get("owner") or u.get("master_id")

            if not isinstance(cost, (int, float)) or cost <= 0:
                continue

            prev_price = monitor_cfg["prices"].get(vkid)
            monitor_cfg["prices"][vkid] = int(cost)

            # Цена упала?
            price_dropped = prev_price is not None and int(cost) < prev_price

            if int(cost) <= monitor_cfg["max_price"] and not is_my_slave(owner):
                if price_dropped:
                    log(f"[мониторинг] {name} (id:{vkid}) "
                        f"цена упала: {format_num(prev_price)} → {format_num(int(cost))} 🪙", "ok")
                else:
                    log(f"[мониторинг] {name} (id:{vkid}) цена: {format_num(int(cost))} 🪙", "info")

                if monitor_cfg["auto_buy"]:
                    result = api_buy(vkid, int(cost))
                    if result and not (result.get("error") or result.get("message")):
                        monitor_cfg["bought_count"] += 1
                        stats["bought_count"] += 1
                        stats["bought_total"] += int(cost)
                        log(f"[мониторинг] ✓ Куплен: {name} за {format_num(int(cost))} 🪙", "ok")
                        # Убираем из мониторинга (уже наш)
                        if vkid in monitor_cfg["targets"]:
                            monitor_cfg["targets"].remove(vkid)
                    else:
                        msg = ""
                        if result:
                            msg = result.get("error") or result.get("message", "")
                        log(f"[мониторинг] ✗ Ошибка покупки {name}: {msg}", "err")

            time.sleep(0.5)  # пауза между запросами внутри цикла

        # Ждём интервал
        for _ in range(monitor_cfg["interval"]):
            if not monitor_cfg["running"]:
                break
            time.sleep(1)

    log("[мониторинг] Остановлен", "info")


_monitor_thread = None


def monitor_start():
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        log("Мониторинг уже запущен", "warn")
        return False
    if not monitor_cfg["targets"]:
        log("Список целей пуст", "warn")
        return False
    monitor_cfg["running"] = True
    _monitor_thread = threading.Thread(target=_monitor_worker, daemon=True)
    _monitor_thread.start()
    log(f"Мониторинг запущен: {len(monitor_cfg['targets'])} целей, "
        f"интервал {monitor_cfg['interval']} сек", "ok")
    return True


def monitor_stop():
    global _monitor_thread
    monitor_cfg["running"] = False
    if _monitor_thread:
        _monitor_thread.join(timeout=3)
        _monitor_thread = None
    log("Мониторинг остановлен", "info")


def monitor_running():
    return _monitor_thread is not None and _monitor_thread.is_alive()


def screen_monitor():
    """Экран настройки мониторинга конкретных игроков."""
    while True:
        clear()
        header("МОНИТОРИНГ ИГРОКОВ  👁")
        print()

        if not sess["user_token"]:
            log("Нет токена", "err")
            input(c(C.GRAY, "\n  Enter...")); return

        running = monitor_running()

        section("Статус")
        row("Мониторинг",    c(C.GREEN, "● работает") if running else c(C.RED, "● остановлен"))
        row("Целей",         c(C.WHITE, str(len(monitor_cfg["targets"]))))
        row("Интервал",      c(C.GRAY,  f"{monitor_cfg['interval']} сек"))
        row("Макс. цена",    c(C.YELLOW, format_num(monitor_cfg["max_price"])) + " 🪙")
        row("Авто-покупка",  c(C.GREEN, "вкл") if monitor_cfg["auto_buy"] else c(C.GRAY, "выкл"))
        row("Проверок",      c(C.GRAY,  str(monitor_cfg["check_count"])))
        row("Куплено",       c(C.CYAN,  str(monitor_cfg["bought_count"])))
        close()

        # Текущие цены
        if monitor_cfg["prices"]:
            print()
            section("Последние цены")
            for vid, price in list(monitor_cfg["prices"].items())[:10]:
                is_cheap = price <= monitor_cfg["max_price"]
                pc = C.GREEN if is_cheap else C.GRAY
                row(f"id:{vid}", c(pc, format_num(price)) + " 🪙")
            close()

        print()
        if running:
            print(f"  {c(C.RED + C.BOLD, ' 1 ')}  {c(C.WHITE, 'Остановить мониторинг')}")
        else:
            print(f"  {c(C.GREEN + C.BOLD, ' 1 ')}  {c(C.WHITE, 'Запустить мониторинг')}")
        print(f"  {c(C.WHITE, ' 2 ')}  Добавить VK ID для мониторинга")
        print(f"  {c(C.WHITE, ' 3 ')}  Очистить список целей")
        print(f"  {c(C.WHITE, ' 4 ')}  Изменить интервал ({monitor_cfg['interval']} сек)")
        print(f"  {c(C.WHITE, ' 5 ')}  Изменить макс. цену")
        print(f"  {c(C.WHITE, ' 6 ')}  Авто-покупка: {'выкл → вкл' if not monitor_cfg['auto_buy'] else 'вкл → выкл'}")
        print(f"  {c(C.GRAY, ' 0 ')}  Назад")

        ch = prompt()

        if ch == "0":
            return
        elif ch == "1":
            if running:
                monitor_stop()
            else:
                if not monitor_cfg["targets"]:
                    log("Сначала добавь цели (пункт 2)", "warn")
                else:
                    monitor_start()
        elif ch == "2":
            v = input(f"  {c(C.GRAY, 'VK ID через запятую')}: ").strip()
            if v:
                new_ids = [int(x.strip()) for x in v.split(",") if x.strip().isdigit()]
                monitor_cfg["targets"].extend(new_ids)
                # Убираем дубликаты
                monitor_cfg["targets"] = list(dict.fromkeys(monitor_cfg["targets"]))
                log(f"Добавлено {len(new_ids)} целей. Всего: {len(monitor_cfg['targets'])}", "ok")
        elif ch == "3":
            monitor_cfg["targets"].clear()
            monitor_cfg["prices"].clear()
            log("Список очищен", "ok")
        elif ch == "4":
            v = input(f"  {c(C.GRAY, 'Интервал (сек, мин 10)')}: ").strip()
            if v.isdigit() and int(v) >= 10:
                monitor_cfg["interval"] = int(v)
                log(f"Интервал: {v} сек", "ok")
            else:
                log("Минимум 10 сек", "warn")
        elif ch == "5":
            v = input(f"  {c(C.GRAY, 'Макс. цена')}: ").strip().replace(" ", "")
            if v.isdigit():
                monitor_cfg["max_price"] = int(v)
                log(f"Макс. цена: {format_num(monitor_cfg['max_price'])} 🪙", "ok")
        elif ch == "6":
            monitor_cfg["auto_buy"] = not monitor_cfg["auto_buy"]
            log(f"Авто-покупка: {'вкл' if monitor_cfg['auto_buy'] else 'выкл'}", "ok")

        if ch != "0":
            time.sleep(0.3)


# ══════════════════════════════════════════════════════
#  ЭКРАН ПРОДАЖИ РАБОВ
# ══════════════════════════════════════════════════════

def screen_sell():
    """Экран продажи рабов — загружает профиль каждого для точной цены продажи."""
    clear()
    header("ПРОДАЖА РАБОВ")
    print()

    log("Загружаю список рабов...")
    data   = api_my_slaves()
    slaves = []
    if isinstance(data, dict):
        slaves = data.get("slaves", [])
    elif isinstance(data, list):
        slaves = data
    slaves = [s for s in slaves if s.get("vkid") != sess["vk_user_id"]]

    if not slaves:
        log("Рабов нет — некого продавать", "warn")
        input(c(C.GRAY, "\n  Enter для возврата...")); return

    # ── Загружаем профиль каждого раба чтобы узнать реальный sale_price ──
    log(f"Проверяю цены продажи ({len(slaves)} рабов)...")
    enriched = []
    for i, s in enumerate(slaves, 1):
        vid = s.get("vkid")
        print(f"\r  {c(C.GRAY, f'[{i}/{len(slaves)}]')} проверяю...", end="", flush=True)
        profile = api_profile(vid)
        if profile and isinstance(profile, dict):
            u          = profile.get("user", profile)
            sale_price = u.get("sale_price", 0) or 0
        else:
            sale_price = 0
        s["_sale_price"] = sale_price
        enriched.append(s)
        time.sleep(0.1)

    print("\r" + " " * 60 + "\r", end="", flush=True)

    can_sell   = [s for s in enriched if s["_sale_price"] > 0]
    cant_sell  = [s for s in enriched if s["_sale_price"] <= 0]

    sorted_can  = sorted(can_sell,  key=lambda x: x["_sale_price"])
    sorted_cant = sorted(cant_sell, key=lambda x: x.get("cost", 0), reverse=True)
    sorted_all  = sorted_can + sorted_cant

    section(f"Рабы ({len(slaves)} чел.)  │  "
            f"Можно продать: {c(C.GREEN + C.BOLD, str(len(can_sell)))}  │  "
            f"Нельзя: {c(C.RED, str(len(cant_sell)))}")
    print(f"  {c(C.CYAN, '│')}")
    print(f"  {c(C.CYAN, '│')}  "
          f"{c(C.GRAY, '  #')}  "
          f"{c(C.GRAY, 'Имя' + ' ' * 17)}  "
          f"{c(C.GRAY, '  Покупная цена')}  "
          f"{c(C.GRAY, '  Цена продажи')}")
    print(f"  {c(C.CYAN, '├' + '─' * (W - 2))}")

    for i, s in enumerate(sorted_all, 1):
        name       = f"{s.get('first_name','')} {s.get('last_name','')}".strip() or f"id:{s.get('vkid','?')}"
        cost       = s.get("cost", 0)
        sale_price = s["_sale_price"]

        if cost < 10_000:    pc = C.GREEN
        elif cost < 100_000: pc = C.YELLOW
        else:                pc = C.WHITE

        if sale_price > 0:
            sell_s = c(C.GREEN + C.BOLD, f"{format_num(sale_price):>14} 🪙")
        else:
            sell_s = c(C.RED, f"{'✗ нельзя':>15}")

        print(f"  {c(C.CYAN, '│')}  "
              f"{c(C.GRAY, f'{i:>3}')}  "
              f"{name[:20]:<20}  "
              f"{c(pc, f'{format_num(cost):>15}')}  "
              f"{sell_s}")
    close()

    if not can_sell:
        print(f"\n  {c(C.YELLOW, '⚠  Нет рабов доступных для продажи')}")
        input(c(C.GRAY, "\n  Enter для возврата...")); return

    print(f"\n  {c(C.GRAY, 'Номер — продать одного  │  A — продать всех доступных  │  0 — выход')}")
    choice = prompt("Номер или A")

    if choice == "0":
        return

    elif choice == "A":
        total_sale = sum(s["_sale_price"] for s in can_sell)
        print(f"\n  Продать {c(C.WHITE + C.BOLD, str(len(can_sell)))} рабов "
              f"и получить {c(C.GREEN + C.BOLD, format_num(total_sale))} 🪙?")
        ans = input(f"  {c(C.RED, '❯')} {c(C.GRAY, 'Подтвердить? (yes)')}: ").strip().lower()
        if ans != "yes":
            log("Отменено", "info")
            input(c(C.GRAY, "\n  Enter...")); return
        print()
        sold = 0
        for j, s in enumerate(can_sell, 1):
            vid        = s.get("vkid")
            name       = f"{s.get('first_name','')} {s.get('last_name','')}".strip() or str(vid)
            sale_price = s["_sale_price"]
            print(f"  {c(C.GRAY, f'[{j}/{len(can_sell)}]')}  "
                  f"{c(C.WHITE, name[:22])}  "
                  f"{c(C.GREEN, format_num(sale_price))} 🪙...",
                  end=" ", flush=True)
            result = api_sell(vid)
            if result and not (result.get("error") or result.get("message")):
                new_bal = result.get("new_balance", "?")
                print(c(C.GREEN, f"✓  баланс: {format_num(new_bal)} 🪙"))
                sold += 1
                stats["sold_count"] += 1
                stats["sold_total"] += sale_price
            else:
                msg = (result.get("error") or result.get("message", "нет ответа")) if result else "нет ответа"
                print(c(C.RED, f"✗ {msg}"))
            time.sleep(cfg["delay_buy"])
        print()
        log(f"Продано {sold} из {len(can_sell)}  +{format_num(stats['sold_total'])} 🪙", "ok")

    elif choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(sorted_all):
            s          = sorted_all[idx]
            vid        = s.get("vkid")
            name       = f"{s.get('first_name','')} {s.get('last_name','')}".strip() or str(vid)
            sale_price = s["_sale_price"]
            if sale_price <= 0:
                log(f"{name} — нельзя продать (цена продажи 0)", "warn")
                input(c(C.GRAY, "\n  Enter...")); return
            print(f"\n  Продать {c(C.WHITE, name)} за {c(C.GREEN + C.BOLD, format_num(sale_price))} 🪙?")
            ans = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, '(y/n)')}: ").strip().lower()
            if ans == "y":
                result = api_sell(vid)
                if result and not (result.get("error") or result.get("message")):
                    new_bal = result.get("new_balance", "?")
                    log(f"Продан: {name}  +{format_num(sale_price)} 🪙  баланс: {format_num(new_bal)}", "ok")
                    stats["sold_count"] += 1
                    stats["sold_total"] += sale_price
                else:
                    msg = (result.get("error") or result.get("message", "")) if result else "нет ответа"
                    log(msg, "err")

    input(c(C.GRAY, "\n  Enter для возврата..."))


# ══════════════════════════════════════════════════════
#  ЭКРАН ТОПА ИГРОКОВ
# ══════════════════════════════════════════════════════

def screen_top():
    """Просмотр топа игроков через API."""
    clear()
    header("ТОП ИГРОКОВ")
    print()

    kinds = [
        ("holders",  "Богатейшие (по монетам)"),
        ("masters",  "Лучшие хозяева (по рабам)"),
        ("income",   "Топ по доходу"),
    ]

    print(f"  {c(C.BOLD, 'Выбери тип топа:')}")
    for i, (k, label) in enumerate(kinds, 1):
        print(f"  {c(C.CYAN, str(i))}  {c(C.WHITE, label)}")
    print(f"  {c(C.GRAY, '0')}  Назад")

    ch = prompt()
    if ch == "0" or not ch.isdigit() or int(ch) < 1 or int(ch) > len(kinds):
        return

    kind, title = kinds[int(ch) - 1]
    print()
    log(f"Загружаю топ: {title}...")
    data = api_top(kind)

    if not data:
        log("Не удалось загрузить топ", "err")
        input(c(C.GRAY, "\n  Enter...")); return

    players = data if isinstance(data, list) else data.get("users", data.get("players", []))
    if not players:
        log("Данные пустые", "warn")
        input(c(C.GRAY, "\n  Enter...")); return

    clear()
    header(f"ТОП: {title.upper()}")
    print()
    section(f"Топ {min(len(players), 50)} игроков")
    print(f"  {c(C.CYAN, '│')}")
    print(f"  {c(C.CYAN, '│')}  "
          f"{c(C.GRAY, '  #')}  "
          f"{c(C.GRAY, '      VK ID')}  "
          f"{c(C.GRAY, 'Имя' + ' ' * 14)}  "
          f"{c(C.GRAY, '        Монеты')}  "
          f"{c(C.GRAY, 'Рабов')}")
    print(f"  {c(C.CYAN, '├' + '─' * (W - 2))}")

    for i, p in enumerate(players[:50], 1):
        name    = f"{p.get('first_name','')} {p.get('last_name','')}".strip() or f"id:{p.get('vkid','?')}"
        vkid_p  = p.get("vkid", "?")
        balance = p.get("balance", p.get("coins", 0))
        s_count = max(0, p.get("slaves_count", 0) or 0)
        is_me_f = p.get("vkid") == sess["vk_user_id"]
        mark    = c(C.CYAN + C.BOLD, "  ◀ ты") if is_me_f else ""

        if i == 1:    nc = C.YELLOW + C.BOLD
        elif i == 2:  nc = C.WHITE  + C.BOLD
        elif i == 3:  nc = C.YELLOW
        elif i <= 10: nc = C.WHITE
        else:         nc = C.GRAY

        print(f"  {c(C.CYAN, '│')}  "
              f"{c(nc, f'{i:>3}')}  "
              f"{c(C.GRAY, f'{vkid_p:>11}')}  "
              f"{name[:17]:<17}  "
              f"{c(C.YELLOW, f'{format_num(balance):>14}')}  "
              f"{c(C.WHITE,  str(s_count))}"
              f"{mark}")
    close()
    input(c(C.GRAY, "\n  Enter для возврата..."))


# ══════════════════════════════════════════════════════
#  КОНФИГ — СОХРАНЕНИЕ / ЗАГРУЗКА НАСТРОЕК
# ══════════════════════════════════════════════════════

CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".slaves_config"
)


def config_save():
    """Сохраняет пользовательские настройки в .slaves_config"""
    data = {
        "cfg": {k: v for k, v in cfg.items() if k != "targets"},
        "cfg_targets": cfg["targets"],
        "scanner": {k: v for k, v in scanner.items()
                    if k not in ("running", "found_list") and not k.startswith("total")},
        "cheap": {k: v for k, v in cheap.items()
                  if k not in ("running", "results", "blacklist") and not k.startswith("total")},
        "monitor": {
            "targets":   monitor_cfg["targets"],
            "max_price": monitor_cfg["max_price"],
            "interval":  monitor_cfg["interval"],
            "auto_buy":  monitor_cfg["auto_buy"],
        },
    }
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log(f"Настройки сохранены в {os.path.basename(CONFIG_FILE)}", "ok")
        return True
    except Exception as ex:
        log(f"Ошибка сохранения: {ex}", "err")
        return False


def config_load():
    """Загружает сохранённые настройки."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # cfg
        for k, v in data.get("cfg", {}).items():
            if k in cfg:
                cfg[k] = v
        targets = data.get("cfg_targets", [])
        if isinstance(targets, list):
            cfg["targets"] = [int(x) for x in targets if str(x).isdigit()]
        # scanner
        for k, v in data.get("scanner", {}).items():
            if k in scanner:
                scanner[k] = v
        # cheap
        for k, v in data.get("cheap", {}).items():
            if k in cheap:
                cheap[k] = v
        # monitor
        mon = data.get("monitor", {})
        for k in ("max_price", "interval", "auto_buy"):
            if k in mon:
                monitor_cfg[k] = mon[k]
        if "targets" in mon and isinstance(mon["targets"], list):
            monitor_cfg["targets"] = [int(x) for x in mon["targets"] if str(x).isdigit()]
        log("Настройки загружены", "ok")
        return True
    except FileNotFoundError:
        return False
    except Exception as ex:
        log(f"Ошибка загрузки настроек: {ex}", "err")
        return False


# ══════════════════════════════════════════════════════
#  ЛОГИ
# ══════════════════════════════════════════════════════

def screen_logs():
    clear(); header("ЛОГИ СЕССИИ"); print()
    if not logs:
        print(f"  {c(C.GRAY, 'Лог пуст')}")
    else:
        for entry in logs[-50:]:
            print(f"  {entry}")
    print()
    print(f"  {c(C.GRAY, f'Показано последних {min(len(logs), 50)} из {len(logs)}')}")
    input(c(C.GRAY, "\n  Enter для возврата..."))


# ══════════════════════════════════════════════════════
#  НАСТРОЙКИ (cfg)
# ══════════════════════════════════════════════════════

def screen_settings():
    while True:
        clear(); header("НАСТРОЙКИ"); print()
        section("Покупки")
        row("1  Макс. цена",       c(C.YELLOW, format_num(cfg["max_price"])) + " 🪙")
        row("2  Макс. покупок",    c(C.WHITE,  str(cfg["max_buy"])))
        row("3  Задержка покупки", c(C.GRAY,   f"{cfg['delay_buy']} сек"))
        row("4  Пропуск кланов",   c(C.GREEN if cfg["skip_clans"] else C.GRAY,
                                    "вкл" if cfg["skip_clans"] else "выкл"))
        divider()
        section("Авто-сбор")
        row("5  Интервал сбора",   c(C.GRAY, f"{cfg['revenue_interval']} сек"))
        row("6  Авто-бонус",       c(C.GREEN if cfg["auto_bonus"] else C.GRAY,
                                    "вкл" if cfg["auto_bonus"] else "выкл"))
        divider()
        section("Цели автоскупки")
        row("   Целей в списке",   c(C.WHITE, str(len(cfg["targets"]))))
        print(f"  {c(C.CYAN, '│')}")
        print(f"  {c(C.CYAN, '│')}  {c(C.WHITE, 'A')}  Добавить VK ID")
        print(f"  {c(C.CYAN, '│')}  {c(C.WHITE, 'D')}  Удалить VK ID")
        print(f"  {c(C.CYAN, '│')}  {c(C.WHITE, 'V')}  Показать список")
        close()
        print(f"\n  {c(C.WHITE, 'W')}  Сохранить настройки в файл")
        print(f"  {c(C.GRAY,  '0')}  Назад")

        ch = prompt("Номер настройки или действие")

        if ch == "0":
            return
        elif ch == "1":
            v = input(f"  {c(C.GRAY, 'Макс. цена')}: ").strip().replace(" ", "").replace("_", "")
            if v.isdigit():
                cfg["max_price"] = int(v); log(f"Макс. цена: {format_num(cfg['max_price'])} 🪙", "ok")
        elif ch == "2":
            v = input(f"  {c(C.GRAY, 'Макс. покупок за сессию')}: ").strip()
            if v.isdigit() and int(v) > 0:
                cfg["max_buy"] = int(v); log(f"Макс. покупок: {v}", "ok")
        elif ch == "3":
            v = input(f"  {c(C.GRAY, 'Задержка между покупками (сек)')}: ").strip()
            try:
                val = float(v)
                if val >= 0: cfg["delay_buy"] = val; log(f"Задержка: {val} сек", "ok")
            except ValueError: log("Введи число", "err")
        elif ch == "4":
            cfg["skip_clans"] = not cfg["skip_clans"]
            log(f"Пропуск кланов: {'вкл' if cfg['skip_clans'] else 'выкл'}", "ok")
        elif ch == "5":
            v = input(f"  {c(C.GRAY, 'Интервал авто-сбора (сек, мин 10)')}: ").strip()
            if v.isdigit() and int(v) >= 10:
                cfg["revenue_interval"] = int(v); log(f"Интервал: {v} сек", "ok")
            else: log("Минимум 10 сек", "warn")
        elif ch == "6":
            cfg["auto_bonus"] = not cfg["auto_bonus"]
            log(f"Авто-бонус: {'вкл' if cfg['auto_bonus'] else 'выкл'}", "ok")
        elif ch == "A":
            v = input(f"  {c(C.GRAY, 'VK ID для добавления')}: ").strip()
            if v.isdigit():
                vid = int(v)
                if vid not in cfg["targets"]:
                    cfg["targets"].append(vid); log(f"Добавлен id:{vid}", "ok")
                else: log("Уже в списке", "warn")
            else: log("Неверный ID", "err")
        elif ch == "D":
            v = input(f"  {c(C.GRAY, 'VK ID для удаления')}: ").strip()
            if v.isdigit():
                vid = int(v)
                if vid in cfg["targets"]:
                    cfg["targets"].remove(vid); log(f"Удалён id:{vid}", "ok")
                else: log("Нет в списке", "warn")
            else: log("Неверный ID", "err")
        elif ch == "V":
            clear(); header("СПИСОК ЦЕЛЕЙ"); print()
            if not cfg["targets"]:
                print(f"  {c(C.GRAY, 'Список пуст')}")
            else:
                for i, vid in enumerate(cfg["targets"], 1):
                    print(f"  {c(C.GRAY, f'{i:>3}')}  {c(C.WHITE, str(vid))}")
            input(c(C.GRAY, "\n  Enter для возврата..."))
        elif ch == "W":
            config_save(); input(c(C.GRAY, "\n  Enter..."))


# ══════════════════════════════════════════════════════
#  ТЕСТ ЦЕНЫ (ЭКСПЕРИМЕНТ)
# ══════════════════════════════════════════════════════

def screen_exploit_test():
    clear(); header("ТЕСТ ЦЕНЫ  (эксперимент)"); print()
    print(f"  {c(C.YELLOW, '⚠  Эксперимент: попытка купить игрока с нестандартной ценой.')}")
    print(f"  {c(C.GRAY,   '   Сервер скорее всего отклонит запрос — смотри ответ.')}")
    print()
    vkid_s = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, 'VK ID цели')}: ").strip()
    if not vkid_s.isdigit(): log("Неверный ID", "err"); input(c(C.GRAY, "\n  Enter...")); return
    vkid = int(vkid_s)
    profile = api_profile(vkid)
    real_cost = None
    if profile:
        u = profile.get("user", profile)
        real_cost = u.get("cost", u.get("sale_price"))
        name = f"{u.get('first_name','')} {u.get('last_name','')}".strip() or str(vkid)
        print(f"\n  {c(C.WHITE, name)}  —  реальная цена: {c(C.YELLOW, format_num(real_cost))} 🪙")
    price_s = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, f'Цена для отправки (Enter = {format_num(real_cost)})')}: ").strip()
    send_price = int(price_s) if price_s.isdigit() else real_cost
    if send_price is None: log("Цена не указана", "err"); input(c(C.GRAY, "\n  Enter...")); return
    print()
    log(f"Отправляю покупку id:{vkid} за {format_num(send_price)}...")
    result = api_buy_raw(vkid, send_price)
    print()
    if result:
        print(f"  {c(C.GRAY, 'Ответ сервера:')}")
        print(f"  {c(C.WHITE, json.dumps(result, ensure_ascii=False, indent=2)[:600])}")
        err_msg = result.get("error") or result.get("message")
        if err_msg: log(err_msg, "err")
        else: log("Успех! (неожиданно)", "ok"); stats["bought_count"] += 1; stats["bought_total"] += send_price
    else:
        log("Нет ответа от сервера", "err")
    input(c(C.GRAY, "\n  Enter для возврата..."))


# ══════════════════════════════════════════════════════
#  РАСШИРЕННАЯ СТАТИСТИКА — дополнение к screen_stats
# ══════════════════════════════════════════════════════

def screen_stats_extended():
    """Расширенный экран статистики + состояние всех модулей."""
    clear()
    header("ПОЛНАЯ СТАТИСТИКА")
    print()

    # Основная сессия
    section("Сессия")
    row("Старт", c(C.WHITE,
        stats["session_start"].strftime("%d.%m.%Y %H:%M:%S")
        if stats["session_start"] else "—"))
    row("Аптайм",     c(C.CYAN,   stats_uptime()))
    row("Ошибок API", c(C.RED if stats["errors"] else C.GRAY, str(stats["errors"])))
    divider()

    # Покупки/продажи
    section("Торговля")
    row("Куплено рабов",  c(C.GREEN,  str(stats["bought_count"])))
    row("Потрачено",      c(C.YELLOW, format_num(stats["bought_total"])) + " 🪙")
    if stats["bought_count"] > 0:
        row("Ср. цена покупки",
            c(C.GRAY, format_num(stats["bought_total"] // stats["bought_count"])) + " 🪙")
    row("Продано рабов",  c(C.GREEN,  str(stats["sold_count"])))
    row("Получено",       c(C.YELLOW, format_num(stats["sold_total"])) + " 🪙")
    divider()

    # Сбор
    section("Монеты")
    row("Собрано через revenue", c(C.YELLOW, format_num(stats["revenue_collected"])) + " 🪙")
    row("Сборов",               c(C.GRAY,   str(stats["revenue_ticks"])))
    row("Бонусов",              c(C.GREEN,  str(stats["bonus_collected"])))
    divider()

    # Авто-сбор
    section("Авто-сбор")
    row("Статус",    c(C.GREEN, "● работает") if auto_revenue_running() else c(C.RED, "● выключен"))
    row("Интервал",  c(C.GRAY, f"{cfg['revenue_interval']} сек"))
    row("Авто-бонус",c(C.GREEN, "вкл") if cfg["auto_bonus"] else c(C.GRAY, "выкл"))
    divider()

    # Парсер дешёвых
    section("Парсер дешёвых")
    row("Просканировано",  c(C.WHITE, format_num(cheap["total_scanned"])))
    row("Найдено",         c(C.GREEN, format_num(len(cheap["results"]))))
    row("Куплено",         c(C.CYAN,  format_num(cheap.get("total_bought", 0))))
    row("Потрачено",       c(C.YELLOW, format_num(cheap.get("total_spent", 0))) + " 🪙")
    row("Последний ID",    c(C.GRAY, format_num(cheap["last_scanned"])))
    divider()

    # Мониторинг
    section("Мониторинг")
    row("Статус",    c(C.GREEN, "● работает") if monitor_running() else c(C.RED, "● остановлен"))
    row("Целей",     c(C.WHITE, str(len(monitor_cfg["targets"]))))
    row("Проверок",  c(C.GRAY,  str(monitor_cfg["check_count"])))
    row("Куплено",   c(C.CYAN,  str(monitor_cfg["bought_count"])))
    divider()

    # Итого
    net = (stats["revenue_collected"] + stats["sold_total"]
           - stats["bought_total"]
           - cheap.get("total_spent", 0))
    net_c = C.GREEN if net >= 0 else C.RED
    row("Нетто P&L", c(net_c + C.BOLD, format_num(net)) + " 🪙")
    close()

    input(c(C.GRAY, "\n  Enter для возврата..."))



# ══════════════════════════════════════════════════════
#  ОБНОВЛЁННОЕ ГЛАВНОЕ МЕНЮ
# ══════════════════════════════════════════════════════

def screen_menu():
    clear()
    header("SLAVES BOT  2.0")
    print()

    auto_dot = " " + c(C.GREEN,  "●") if auto_revenue_running() else ""
    mon_dot  = " " + c(C.YELLOW, "◉") if monitor_running()      else ""

    # Вспомогательные функции рендера
    def grp(title):
        print(f"  {c(C.GRAY, chr(9474))}")
        print(f"  {c(C.GRAY, chr(9474))}  {c(C.GRAY, title.upper())}")

    def item(key, label, color=C.WHITE):
        print(f"  {c(C.GRAY, chr(9474))}    {c(C.CYAN + C.BOLD, f'[{key}]')}  {c(color, label)}")

    print(f"  {c(C.GRAY, chr(9484) + chr(9472) * (W - 2))}")

    # ── Профиль ──────────────────────────────────────
    grp("👤  аккаунт")
    item("1", "Мой профиль")
    item("2", "Мои рабы")
    item("9", "Поиск по VK ID")
    item("O", "Топ игроков")

    # ── Монеты ───────────────────────────────────────
    grp("🪙  монеты")
    item("3", "Собрать монеты")
    item("4", "Ежедневный бонус")
    item("6", "Авто-сбор монет" + auto_dot,
         C.GREEN if auto_revenue_running() else C.WHITE)

    # ── Торговля ─────────────────────────────────────
    grp("⚡  покупка / продажа")
    item("B", "Ручная покупка")
    item("5", "Автоскупка (рабы цели)")
    item("V", "Продажа рабов")

    # ── Сканеры ──────────────────────────────────────
    grp("🔍  сканеры")
    item("P", "Сканер диапазона")
    item("Q", "Парсер дешёвых  💰")
    item("N", "Мониторинг игроков" + mon_dot,
         C.YELLOW if monitor_running() else C.WHITE)

    # ── Статистика ───────────────────────────────────
    grp("📊  статистика")
    item("7", "Статистика сессии")
    item("8", "Расширенная статистика")
    item("L", "Логи")

    # ── Система ──────────────────────────────────────
    grp("⚙  система")
    item("S", "Настройки")
    item("W", "Сохранить настройки")
    item("M", "Ввести x-init-data")
    item("X", "Тест цены (эксперимент)", C.GRAY)
    item("0", "Выход", C.GRAY)

    print(f"  {c(C.GRAY, chr(9474))}")
    print(f"  {c(C.GRAY, chr(9492) + chr(9472) * (W - 2))}")
    status_bar()


# ══════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════

def _input_new_init_data():
    """Ввод нового x-init-data."""
    print(f"  {c(C.GRAY, 'Как получить x-init-data:')}")
    print(f"  {c(C.CYAN, '1.')} Открой {c(C.WHITE, 'vk.com/app7804694')}")
    print(f"  {c(C.CYAN, '2.')} Нажми {c(C.WHITE, 'F12')} → вкладка {c(C.WHITE, 'Network')}")
    print(f"  {c(C.CYAN, '3.')} Найди запрос {c(C.WHITE, 'auth?ts=...')}")
    print(f"  {c(C.CYAN, '4.')} Headers → скопируй {c(C.WHITE, 'x-init-data')}")
    print()
    xi = input(f"  {c(C.CYAN, '❯')} {c(C.GRAY, 'Вставь x-init-data')}: ").strip()
    if not xi:
        log("Пусто", "warn"); return
    sess["x_init_data"]  = xi
    sess["check_string"] = urllib.parse.unquote(xi)
    sess["user_token"]   = ""
    sess["profile"]      = None
    print()
    log("Авторизуюсь...")
    result = api_auth()
    if result and sess["user_token"]:
        log(f"Токен получен: {sess['user_token'][:20]}...", "ok")
        log("x-init-data сохранён — больше вводить не придётся", "ok")
    else:
        log("Авторизация не удалась — проверь x-init-data", "err")
        clear_session()


def screen_auth():
    clear()
    header("АВТОРИЗАЦИЯ")
    print()
    has_xid = bool(sess["x_init_data"])
    has_tok = bool(sess["user_token"])
    has_cs  = bool(sess["check_string"])
    if has_xid:
        section("Сохранённые данные")
        row("x-init-data",  c(C.GREEN, sess["x_init_data"][:40] + "..."))
        row("check_string", c(C.GREEN, f"OK ({len(sess['check_string'])} сим.)") if has_cs else c(C.RED, "ПУСТО!"))
        row("Токен",        c(C.GREEN, sess["user_token"][:20] + "...") if has_tok else c(C.RED, "нет"))
        close()
        print()
        print(f"  {c(C.GRAY, 'x-init-data не протухает — через него можно получать новые токены')}")
        print()
        print(f"  {c(C.WHITE, '1')}  Переавторизоваться (новый токен из текущего x-init-data)")
        print(f"  {c(C.WHITE, '2')}  Ввести новый x-init-data")
        print(f"  {c(C.WHITE, '3')}  Полный сброс сессии")
        print(f"  {c(C.WHITE, '0')}  Назад")
        print()
        choice = prompt("Выбери действие")
        if choice == "1":
            print()
            if reauth(): log("Готово — новый токен получен", "ok")
            else: log("Не удалось — попробуй ввести новый x-init-data", "err")
        elif choice == "2":
            _input_new_init_data()
        elif choice == "3":
            clear_session()
            log("Сессия полностью очищена", "ok")
    else:
        _input_new_init_data()
    input(c(C.GRAY, "\n  Enter для возврата..."))



def main():
    sess["x_version"] = int_to_base32(VK_USER_ID)
    if os.name == "nt": os.system("color")

    # Пробуем загрузить конфиг настроек
    config_load()

    clear(); header("SLAVES BOT  2.0"); print()
    if load_session():
        has_xid = bool(sess["x_init_data"])
        has_tok = bool(sess["user_token"])
        has_cs  = bool(sess["check_string"])
        if has_xid: log(f"x-init-data: OK ({len(sess['x_init_data'])} симв.)", "ok")
        else:       log("x-init-data: ОТСУТСТВУЕТ!", "err")
        if has_cs:  log("check_string: OK", "ok")
        else:       log("check_string: ПУСТО!", "err")
        if has_tok and has_cs:
            log("Проверяю токен...")
            if validate_token(): log("Сессия активна", "ok")
            else:
                if sess["user_token"]: log("Сессия восстановлена (новый токен)", "ok")
                else: log("Введи новый x-init-data через M", "warn")
        elif has_xid and not has_tok:
            log("Токена нет, получаю через x-init-data...")
            if reauth(): log("Сессия активна", "ok")
            else: log("Не удалось — введи x-init-data через M", "warn")
        else:
            log("Введи x-init-data через M для начала работы", "info")
    else:
        log("Сессии нет — введи x-init-data через M", "info")

    time.sleep(1.2)
    stats_reset()

    # Инициализируем поля парсера дешёвых
    cheap.setdefault("total_bought", 0)
    cheap.setdefault("total_spent",  0)
    cheap.setdefault("blacklist",    set())
    cheap.setdefault("max_price",    500_000)
    cheap.setdefault("auto_buy",     False)

    # Главный цикл
    while True:
        screen_menu()
        choice = prompt()

        if choice == "0":
            if auto_revenue_running(): auto_revenue_stop()
            if monitor_running():      monitor_stop()
            config_save()
            clear()
            print(f"\n  {c(C.GRAY, 'До свидания.')}\n")
            sys.exit(0)
        elif choice == "1":  screen_profile()
        elif choice == "2":  screen_my_slaves()
        elif choice == "3":  screen_revenue()
        elif choice == "4":  screen_daily_bonus()
        elif choice == "5":  screen_autobuy()
        elif choice == "6":  screen_auto_revenue()
        elif choice == "7":  screen_stats()
        elif choice == "8":  screen_stats_extended()
        elif choice == "9":  screen_lookup()
        elif choice == "P":  screen_scanner()
        elif choice == "Q":  screen_cheapest()
        elif choice == "N":  screen_monitor()
        elif choice == "V":  screen_sell()
        elif choice == "O":  screen_top()
        elif choice == "B":  screen_manual_buy()
        elif choice == "X":  screen_exploit_test()
        elif choice == "L":  screen_logs()
        elif choice == "S":  screen_settings()
        elif choice == "W":  config_save(); input(c(C.GRAY, "\n  Enter..."))
        elif choice == "M":  screen_auth()


if __name__ == "__main__":
    main()
    