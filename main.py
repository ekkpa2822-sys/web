"""
Slaves Web Panel — FastAPI + Jinja2 + SQLite auth
pip install fastapi uvicorn jinja2 python-multipart requests
"""
import asyncio, json, os, threading, time, logging, traceback
from datetime import datetime
from typing import Optional

# ── Logging ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("slaves")

import uvicorn
from fastapi import FastAPI, Request, Form, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import slaves_api as api
import scheduler
import auth_db

app = FastAPI(title="Slaves Panel")

_BASE       = os.path.dirname(os.path.abspath(__file__))
_static_dir = os.path.join(_BASE, "static")
os.makedirs(_static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=_static_dir), name="static")
templates = Jinja2Templates(directory=os.path.join(_BASE, "templates"))

# ── Scanner state ───────────────────────────────────────────
scanner_state = {"running":False,"progress":0,"total":0,"scanned":0,
                 "found":[],"bought":0,"spent":0,"errors":0,"last_id":0,"start_time":None,
                 "started_by":None}  # user_id who started
scanner_ws_clients = set()
_main_loop = None

# ── Startup ─────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global _main_loop
    _main_loop = asyncio.get_event_loop()
    api.stats["start"] = datetime.now()
    # Per-user sessions — no global VK session loaded at startup
    print("[startup] Multi-user mode: each user loads their own VK session")
    auth_db.cleanup_sessions()

# ══════════════════════════════════════════════════════
#  AUTH HELPERS
# ══════════════════════════════════════════════════════

def get_current_user(request: Request) -> Optional[dict]:
    token = request.cookies.get(auth_db.COOKIE_NAME)
    if not token:
        return None
    # get_session now returns full user data — no need for second query
    return auth_db.get_session(token)

def require_login(request: Request):
    """Returns (redirect_or_None, user_or_None)"""
    user = get_current_user(request)
    if not user or not user.get("is_active"):
        return RedirectResponse("/login", status_code=302), None
    return None, user

# In-memory token cache: {user_id: user_token}
_token_cache: dict = {}
# Per-user VK sessions: {user_id: {x_init_data, check_string, user_token, vk_user_id, x_version}}
_user_sessions: dict = {}
# Lock to prevent concurrent session overwrites
import threading as _threading
_session_lock = _threading.Lock()
# Brute-force tracking: {ip_key: (fail_count, last_fail_ts)}
_login_fails: dict = {}

# In-memory profile cache: {user_id: (timestamp, profile_data)}
_profile_cache: dict = {}
PROFILE_CACHE_TTL = 60  # seconds — refresh profile data every 60s max

def get_cached_profile(user_id: int):
    """Return cached api_init() result, or None if expired."""
    entry = _profile_cache.get(user_id)
    if not entry:
        return None
    ts, data = entry
    if time.time() - ts > PROFILE_CACHE_TTL:
        return None
    return data

def set_cached_profile(user_id: int, data):
    _profile_cache[user_id] = (time.time(), data)

def load_user_session(user: dict) -> bool:
    """Load this user's own VK session into api.sess.
    Token is cached in memory — no reauth spam on every request."""
    xi = user.get("x_init_data", "")
    if not xi:
        return False
    import urllib.parse as _up

    uid    = user["id"]
    vk_uid = user.get("vk_uid") or 0

    # Set ALL session fields BEFORE any request
    api.sess["x_init_data"]  = xi
    api.sess["check_string"] = _up.unquote(xi)
    api.sess["vk_user_id"]   = vk_uid
    api.sess["vk_first"]     = user.get("vk_first", "")
    api.sess["vk_last"]      = user.get("vk_last",  "")

    # If vk_uid unknown — parse it from x_init_data right now
    if not vk_uid:
        parsed = api.parse_xi_user(xi)
        if parsed.get("vk_user_id"):
            vk_uid = parsed["vk_user_id"]
            api.sess["vk_user_id"] = vk_uid
            if not api.sess["vk_first"]: api.sess["vk_first"] = parsed.get("vk_first","")
            if not api.sess["vk_last"]:  api.sess["vk_last"]  = parsed.get("vk_last","")
            log.info(f"[session] parsed vk_uid={vk_uid} from xi")
            # Save back to DB
            auth_db.update_user(uid, vk_uid=vk_uid,
                vk_first=api.sess["vk_first"], vk_last=api.sess["vk_last"])

    # x_version MUST be set before any API call
    if vk_uid:
        api.sess["x_version"] = api.int_to_base32(vk_uid)

    log.info(f"[session] loaded uid={uid} vk_uid={vk_uid} x_version={api.sess.get('x_version')}")

    # Restore cached token for THIS user
    cached_token = _token_cache.get(uid, "")
    api.sess["user_token"] = cached_token

    # Also cache in _user_sessions for isolation
    _user_sessions[uid] = {
        "x_init_data":  xi,
        "check_string": api.sess["check_string"],
        "user_token":   cached_token,
        "vk_user_id":   vk_uid,
        "x_version":    api.sess.get("x_version",""),
        "vk_first":     api.sess.get("vk_first",""),
        "vk_last":      api.sess.get("vk_last",""),
    }

    if api.is_ok():
        return True

    log.info(f"[session] no token for uid={uid}, calling reauth()")
    ok = api.reauth()
    if ok:
        _token_cache[uid] = api.sess["user_token"]
        _user_sessions[uid]["user_token"] = api.sess["user_token"]
        log.info(f"[session] reauth ok, token cached for uid={uid}")
        return True
    log.warning(f"[session] reauth FAILED for uid={uid}")
    return False

def require_auth(request: Request):
    """Needs panel login + active status + VK session."""
    redir, user = require_login(request)
    if redir:
        return redir
    status = user.get("status", "pending_xi")
    if status == "pending_xi":
        return RedirectResponse("/setup", status_code=302)
    if status == "pending_approval":
        return RedirectResponse("/setup?submitted=1", status_code=302)
    # Admin can access panel without VK session (sets it via Profile)
    if user.get("role") == "admin":
        if user.get("x_init_data"):
            load_user_session(user)
        return None
    # Regular user — must have working VK session
    if user.get("x_init_data"):
        ok = load_user_session(user)
        if not ok:
            return RedirectResponse("/setup", status_code=302)
    elif not user.get("x_init_data"):
        return RedirectResponse("/setup", status_code=302)
    return None

def require_admin(request: Request):
    redir, user = require_login(request)
    if redir:
        return redir, None
    if user.get("role") != "admin":
        return RedirectResponse("/dashboard", status_code=302), None
    return None, user

# ══════════════════════════════════════════════════════
#  PAGES — AUTH
# ══════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    redir, user = require_login(request)
    if redir:
        return redir
    return RedirectResponse("/dashboard")

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = get_current_user(request)
    if user and api.is_ok():
        return RedirectResponse("/dashboard")
    return templates.TemplateResponse("login.html", {
        "request": request, "login_error": None, "reg_error": None, "reg_ok": False,
        "recaptcha_site_key": auth_db.RECAPTCHA_SITE_KEY,
        "sess": {},
    })

@app.post("/login", response_class=HTMLResponse)
async def login_post(request: Request):
    form = await request.form()
    username   = str(form.get("username","")).strip().lower()
    password   = str(form.get("password",""))
    remember   = form.get("remember")
    cap_token  = str(form.get("g-recaptcha-response",""))
    ctx = {"request":request,"login_error":None,"reg_error":None,"reg_ok":False,
           "recaptcha_site_key":auth_db.RECAPTCHA_SITE_KEY}
    if not auth_db.verify_recaptcha(cap_token, request.client.host):
        ctx["login_error"] = "Пройди reCAPTCHA"
        return templates.TemplateResponse("login.html", ctx)
    # Brute-force protection: max 5 failed attempts per IP per 5 minutes
    ip = request.client.host or "unknown"
    fail_key = f"fail:{ip}"
    fail_count = _login_fails.get(fail_key, (0, 0))
    now_ts = time.time()
    if fail_count[0] >= 5 and now_ts - fail_count[1] < 300:
        ctx["login_error"] = "Слишком много попыток — подожди 5 минут"
        return templates.TemplateResponse("login.html", ctx)

    user = auth_db.get_user_by_username(username)
    if not user or not auth_db.check_password(password, user["pass_hash"]):
        # Increment fail counter
        count = fail_count[0] + 1 if now_ts - fail_count[1] < 300 else 1
        _login_fails[fail_key] = (count, now_ts)
        ctx["login_error"] = f"Неверный логин или пароль (попытка {count}/5)"
        auth_db.audit(0, username, "LOGIN_FAIL", detail=f"ip={ip} attempt={count}")
        return templates.TemplateResponse("login.html", ctx)
    # Reset fail counter on success
    _login_fails.pop(fail_key, None)
    if not user["is_active"]:
        ctx["login_error"] = "Аккаунт заблокирован"
        return templates.TemplateResponse("login.html", ctx)
    token = auth_db.create_session(user["id"], request.client.host, request.headers.get("user-agent",""))
    api.add_log(f"Вход: {user['username']} ({request.client.host})", "ok")
    auth_db.audit(user["id"], user["username"], "LOGIN", detail=f"ip={request.client.host}")
    resp = RedirectResponse("/dashboard", status_code=302)
    resp.set_cookie(auth_db.COOKIE_NAME, token, httponly=True,
                    max_age=60*60*24*7 if remember else None, samesite="lax")
    return resp

@app.post("/register", response_class=HTMLResponse)
async def register_post(request: Request):
    form = await request.form()
    username  = str(form.get("username","")).strip().lower()
    password  = str(form.get("password",""))
    password2 = str(form.get("password2",""))
    email     = str(form.get("email","")).strip() or None
    cap_token = str(form.get("g-recaptcha-response",""))
    ctx = {"request":request,"login_error":None,"reg_error":None,"reg_ok":False,
           "recaptcha_site_key":auth_db.RECAPTCHA_SITE_KEY}
    if not auth_db.verify_recaptcha(cap_token, request.client.host):
        ctx["reg_error"] = "Пройди reCAPTCHA"
        return templates.TemplateResponse("login.html", ctx)
    if len(username) < 3:
        ctx["reg_error"] = "Логин минимум 3 символа"
        return templates.TemplateResponse("login.html", ctx)
    if len(password) < 6:
        ctx["reg_error"] = "Пароль минимум 6 символов"
        return templates.TemplateResponse("login.html", ctx)
    if password != password2:
        ctx["reg_error"] = "Пароли не совпадают"
        return templates.TemplateResponse("login.html", ctx)
    user = auth_db.create_user(username, password, email)
    if not user:
        ctx["reg_error"] = "Логин или email уже занят"
        return templates.TemplateResponse("login.html", ctx)
    api.add_log(f"Регистрация: {username}", "ok")
    ctx["reg_ok"] = True
    return templates.TemplateResponse("login.html", ctx)

@app.get("/logout")
async def logout(request: Request):
    token = request.cookies.get(auth_db.COOKIE_NAME)
    if token:
        auth_db.delete_session(token)
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie(auth_db.COOKIE_NAME)
    return resp

# ══════════════════════════════════════════════════════
#  SETUP PAGE — submit x-init-data
# ══════════════════════════════════════════════════════

@app.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request, submitted: str = ""):
    redir, user = require_login(request)
    if redir: return redir
    status = user.get("status","pending_xi")
    if status == "active":
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse("setup.html", {
        "request": request,
        "user": user,
        "submitted": submitted == "1",
        "status": status,
        "sess": {
            "vk_first": user.get("username",""),
            "vk_last": "",
            "vk_user_id": user.get("vk_uid",""),
            "panel_role": user.get("role","user"),
            "panel_user": user.get("username",""),
        },
    })

@app.post("/api/submit-xi")
async def submit_xi(request: Request):
    redir, user = require_login(request)
    if isinstance(redir, RedirectResponse): return JSONResponse({"error": "not_auth"}, 401)
    body = await request.json()
    xi   = body.get("x_init_data","").strip()
    if len(xi) < 30:
        return JSONResponse({"error": "Слишком короткое значение"})
    # Save as pending, wait for admin
    auth_db.update_user(user["id"], xi_token=xi, status="pending_approval")
    # Clear cached token — will reauth with new xi after approval
    _token_cache.pop(user["id"], None)
    return JSONResponse({"ok": True})

@app.get("/api/check-status")
async def check_status(request: Request):
    redir, user = require_login(request)
    if isinstance(redir, RedirectResponse): return JSONResponse({"status": "not_auth"})
    return JSONResponse({"status": user.get("status","pending_xi")})

@app.post("/api/admin/approve/{uid}")
async def admin_approve(uid: int, request: Request):
    redir, admin = require_admin(request)
    if redir: return JSONResponse({"error": "forbidden"}, 403)
    user = auth_db.get_user_by_id(uid)
    if not user: return JSONResponse({"error": "Пользователь не найден"})
    xi = user.get("xi_token","")
    if not xi: return JSONResponse({"error": "Нет x-init-data"})
    # Test the session before approving
    import urllib.parse as _up
    api.sess["x_init_data"]  = xi
    api.sess["check_string"] = _up.unquote(xi)
    api.sess["user_token"]   = ""
    ok = api.reauth()
    if ok:
        d = api.api_init()
        vk_uid = d.get("vkid") if d else None
        auth_db.update_user(uid,
            status="active",
            x_init_data=xi,
            xi_token="",
            approved_by=admin["id"],
            approved_at=datetime.now().isoformat(),
            vk_uid=vk_uid,
        )
        # Cache the token for this user
        _token_cache[uid] = api.sess["user_token"]
        api.add_log(f"Пользователь {user['username']} одобрен администратором", "ok")
        auth_db.audit(admin["id"], admin["username"], "ADMIN_APPROVE",
            target=f"user:{user['username']}(id:{uid})", detail=f"vk_uid={vk_uid}")
        return JSONResponse({"ok": True, "vk_uid": vk_uid})
    else:
        return JSONResponse({"error": "x-init-data не прошёл проверку — токен недействителен"})

@app.post("/api/admin/reject/{uid}")
async def admin_reject(uid: int, request: Request):
    redir, admin = require_admin(request)
    if redir: return JSONResponse({"error": "forbidden"}, 403)
    auth_db.update_user(uid, status="pending_xi", xi_token="")
    return JSONResponse({"ok": True})

# ══════════════════════════════════════════════════════
#  PROFILE PAGE
# ══════════════════════════════════════════════════════

@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    try:
        redir, user = require_login(request)
        if redir: return redir
        uid = user["id"]
        log.info(f"[profile] user={user.get('username')} uid={uid} has_xi={bool(user.get('x_init_data'))}")

        # Load VK session for this user
        if user.get("x_init_data"):
            load_user_session(user)

        vk_profile = get_cached_profile(uid)
        if vk_profile is None and api.is_ok():
            log.info(f"[profile] fetching api_init for uid={uid}")
            vk_profile = api.api_init()
            if vk_profile:
                set_cached_profile(uid, vk_profile)
                log.info(f"[profile] api_init ok: vkid={vk_profile.get('vkid')}")
            else:
                log.warning(f"[profile] api_init returned None")

        sess_ctx = {
            "panel_role":  user.get("role", "user"),
            "vk_first":    user.get("username", ""),
            "vk_last":     "",
            "vk_user_id":  user.get("vk_uid", ""),
        }
        return templates.TemplateResponse("profile.html", {
            "request":      request,
            "current_user": user,
            "vk_profile":   vk_profile,
            "sess":         sess_ctx,
            "fmt":          api.fmt,
            "fmt_date":     api.fmt_date,
        })
    except Exception as e:
        log.error(f"[profile] CRASH: {e}")
        log.error(traceback.format_exc())
        return HTMLResponse(f"<pre style='color:red;background:#000;padding:20px'>[PROFILE ERROR]\n{traceback.format_exc()}</pre>", status_code=500)

@app.get("/profile/{uid}", response_class=HTMLResponse)
async def profile_uid_page(uid: int, request: Request):
    """Admin-only: view any user profile by UID."""
    redir, admin = require_admin(request)
    if redir: return RedirectResponse("/login", status_code=302)

    target = auth_db.get_user_by_id(uid)
    if not target:
        return RedirectResponse("/admin", status_code=302)

    # Load VK profile using target user's x_init_data (NOT showing xi to template)
    vk_profile  = None
    vk_slaves   = []
    xi_valid    = bool(target.get("x_init_data"))
    if xi_valid and target.get("vk_uid"):
        import urllib.parse as _up
        # Temporarily use target's session to fetch their VK data
        old_sess = dict(api.sess)
        api.sess["x_init_data"]  = target["x_init_data"]
        api.sess["check_string"] = _up.unquote(target["x_init_data"])
        api.sess["vk_user_id"]   = target["vk_uid"]
        api.sess["x_version"]    = api.int_to_base32(target["vk_uid"])
        api.sess["user_token"]   = ""
        api.reauth()
        if api.is_ok():
            vk_profile = api.api_init()
            sd = api.api_my_slaves()
            if isinstance(sd, dict):   vk_slaves = sd.get("slaves", [])
            elif isinstance(sd, list): vk_slaves = sd
        # Restore original session
        for k, v in old_sess.items():
            api.sess[k] = v

    # Hide x_init_data — pass only metadata
    target_safe = {k: v for k, v in target.items() if k not in ("x_init_data", "xi_token", "pass_hash", "user_token")}
    target_safe["has_xi"] = xi_valid

    return templates.TemplateResponse("profile_uid.html", {
        "request":    request,
        "admin":      admin,
        "target":     target_safe,
        "vk_profile": vk_profile,
        "vk_slaves":  sorted(vk_slaves, key=lambda x: x.get("cost",0), reverse=True)[:20],
        "xi_valid":   xi_valid,
        "sess":       {"panel_role": admin["role"], "vk_first": admin.get("username",""), "vk_last": "", "vk_user_id": ""},
        "fmt":        api.fmt,
        "fmt_date":   api.fmt_date,
    })

# ══════════════════════════════════════════════════════
#  ADMIN PANEL
# ══════════════════════════════════════════════════════

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    redir, user = require_admin(request)
    if redir: return redir
    all_users = auth_db.get_all_users()
    pending   = [u for u in all_users if u.get("status") == "pending_approval"]
    return templates.TemplateResponse("admin.html", {
        "request":      request,
        "current_user": user,
        "users":        all_users,
        "pending":      pending,
        "total_users":  len(all_users),
        "admins":       sum(1 for u in all_users if u["role"] == "admin"),
        "banned":       sum(1 for u in all_users if not u["is_active"]),
        "sess_count":   auth_db.count_sessions().get("active", 0),
        "sessions":     auth_db.count_sessions(),
        "sess":         {"panel_role": user["role"], "vk_first": user.get("username",""), "vk_last": "", "vk_user_id": ""},
        "logs":         list(reversed(api.event_log[-30:])),
        "uptime":       api.uptime(),
        "fmt":          api.fmt,
    })

@app.post("/api/admin/check-xi/{uid}")
async def admin_check_xi(uid: int, request: Request):
    """Check if user's x-init-data is valid — without exposing it."""
    redir, admin = require_admin(request)
    if redir: return JSONResponse({"error": "not_admin"}, 403)
    target = auth_db.get_user_by_id(uid)
    if not target: return JSONResponse({"error": "not_found"})
    xi = target.get("x_init_data") or target.get("xi_token","")
    if not xi: return JSONResponse({"valid": False, "error": "no_xi"})
    import urllib.parse as _up
    old_sess = dict(api.sess)
    api.sess["x_init_data"]  = xi
    api.sess["check_string"] = _up.unquote(xi)
    api.sess["vk_user_id"]   = target.get("vk_uid", 0)
    if api.sess["vk_user_id"]:
        api.sess["x_version"] = api.int_to_base32(api.sess["vk_user_id"])
    api.sess["user_token"] = ""
    ok = api.reauth()
    vk_uid = None
    if ok:
        d = api.api_init()
        if d: vk_uid = d.get("vkid")
    for k, v in old_sess.items():
        api.sess[k] = v
    return JSONResponse({"valid": ok, "vk_uid": vk_uid})

@app.post("/api/admin/user/create")
async def admin_create_user(request: Request):
    redir, user = require_admin(request)
    if redir: return JSONResponse({"error": "not_admin"}, 403)
    body = await request.json()
    username = str(body.get("username","")).strip().lower()
    password = str(body.get("password",""))
    email    = body.get("email") or None
    role     = body.get("role","user")
    if len(username) < 3 or len(password) < 6:
        return JSONResponse({"error": "Логин мин 3, пароль мин 6"})
    new_user = auth_db.create_user(username, password, email, role)
    if not new_user:
        return JSONResponse({"error": "Логин или email уже занят"})
    return JSONResponse({"ok": True, "id": new_user["id"]})

@app.post("/api/admin/user/{uid}")
async def admin_edit_user(uid: int, request: Request):
    redir, admin = require_admin(request)
    if redir: return JSONResponse({"error": "not_admin"}, 403)
    body   = await request.json()
    action = body.get("action", "")
    if action == "toggle":
        if uid == admin["id"]: return JSONResponse({"error": "Нельзя заблокировать себя"})
        target = auth_db.get_user_by_id(uid)
        if not target: return JSONResponse({"error": "Not found"})
        new_state = 0 if target["is_active"] else 1
        auth_db.update_user(uid, is_active=new_state)
        action_str = "BAN" if new_state==0 else "UNBAN"
        auth_db.audit(admin["id"], admin["username"], f"ADMIN_{action_str}",
            target=f"user:{target['username']}(id:{uid})")
        return JSONResponse({"ok": True})
    if action == "reset_password":
        pw = body.get("password","")
        if len(pw) < 6: return JSONResponse({"error": "Min 6 chars"})
        auth_db.update_password(uid, pw)
        return JSONResponse({"ok": True})
    if action == "set_role":
        role = body.get("role","user")
        if role not in ("user","admin"): return JSONResponse({"error": "Invalid role"})
        if uid == admin["id"]: return JSONResponse({"error": "Cannot change own role"})
        auth_db.update_user(uid, role=role)
        return JSONResponse({"ok": True})
    # Auto-generate password
    if body.get("reset_password"):
        import random, string
        new_pw = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        auth_db.update_password(uid, new_pw)
        return JSONResponse({"ok": True, "new_password": new_pw})
    fields = {}
    if body.get("email") is not None: fields["email"] = body["email"] or None
    if body.get("role"):              fields["role"]  = body["role"]
    if fields: auth_db.update_user(uid, **fields)
    if body.get("password") and len(body["password"]) >= 6:
        auth_db.update_password(uid, body["password"])
    return JSONResponse({"ok": True})

@app.delete("/api/admin/user/{uid}")
async def admin_delete_user(uid: int, request: Request):
    redir, admin = require_admin(request)
    if redir: return JSONResponse({"error": "not_admin"}, 403)
    if uid == admin["id"]:
        return JSONResponse({"error": "Нельзя удалить себя"})
    auth_db.delete_user(uid)
    return JSONResponse({"ok": True})

@app.post("/api/admin/user/{uid}/ban")
async def admin_ban_user(uid: int, request: Request):
    redir, admin = require_admin(request)
    if redir: return JSONResponse({"error": "not_admin"}, 403)
    body = await request.json()
    auth_db.update_user(uid, is_active=1 if body.get("activate") else 0)
    return JSONResponse({"ok": True})

# ══════════════════════════════════════════════════════
#  PROFILE API
# ══════════════════════════════════════════════════════

@app.post("/api/profile/update")
async def profile_update(request: Request):
    redir, user = require_login(request)
    if redir: return JSONResponse({"error": "not_logged"}, 401)
    body = await request.json()
    fields = {}
    # Only allow safe fields — no role/status changes via profile
    if "email" in body:
        email = str(body["email"]).strip() if body["email"] else None
        fields["email"] = email
    if fields:
        auth_db.update_user(user["id"], **fields)
        auth_db.audit(user["id"], user["username"], "PROFILE_UPDATE", detail=str(list(fields.keys())))
    return JSONResponse({"ok": True})

@app.post("/api/profile/password")
async def profile_password(request: Request):
    redir, user = require_login(request)
    if redir: return JSONResponse({"error": "not_logged"}, 401)
    body = await request.json()
    old_pw = body.get("old_password","")
    new_pw = body.get("new_password","")
    full   = auth_db.get_user_by_id(user["id"])
    if not auth_db.check_password(old_pw, full["pass_hash"]):
        return JSONResponse({"error": "Неверный текущий пароль"})
    if len(new_pw) < 6:
        return JSONResponse({"error": "Новый пароль мин 6 символов"})
    auth_db.update_password(user["id"], new_pw)
    return JSONResponse({"ok": True})

# ══════════════════════════════════════════════════════
#  MAIN PAGES
# ══════════════════════════════════════════════════════


@app.get("/api/admin/get-xi/{uid}")
async def admin_get_xi(uid: int, request: Request):
    """Admin only: return raw x-init-data for a user. Logged."""
    redir, admin = require_admin(request)
    if redir: return JSONResponse({"error": "forbidden"}, 403)
    target = auth_db.get_user_by_id(uid)
    if not target: return JSONResponse({"error": "not_found"})
    xi = target.get("x_init_data") or target.get("xi_token", "")
    if not xi: return JSONResponse({"error": "no_xi"})
    # Audit: log who accessed xi of whom
    auth_db.audit(admin["id"], admin["username"], "VIEW_XI",
        target=f"user:{target['username']}(id:{uid})",
        ip=request.client.host)
    log.warning(f"[SECURITY] {admin['username']} viewed x-init-data of {target['username']}")
    return JSONResponse({"xi": xi})




@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    r = require_auth(request)
    if r: return r
    redir, user = require_login(request)
    trades = auth_db.get_trade_history(user_id=user["id"], limit=500)
    total_spent = sum(t["price"] or 0 for t in trades if t["action"]=="buy")
    total_earned = sum(t["price"] or 0 for t in trades if t["action"]=="sell")
    return templates.TemplateResponse("history.html", {
        "request": request,
        "trades":  trades,
        "total_spent":  total_spent,
        "total_earned": total_earned,
        "sess": {
            "panel_role": user.get("role","user"),
            "vk_first":   user.get("username",""),
            "vk_last":    "",
            "vk_user_id": user.get("vk_uid",""),
        },
        "fmt": api.fmt,
        "current_user": user,
    })

@app.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request):
    redir, admin = require_admin(request)
    if redir: return redir
    logs = auth_db.get_audit_log(limit=500)
    return templates.TemplateResponse("audit.html", {
        "request": request,
        "logs":    logs,
        "sess": {
            "panel_role": admin.get("role","admin"),
            "vk_first":   admin.get("username",""),
            "vk_last":    "",
            "vk_user_id": admin.get("vk_uid",""),
        },
        "current_user": admin,
    })


@app.get("/api/profile/xi-status")
async def xi_status(request: Request):
    """Check if current user's VK session is still valid."""
    redir, user = require_login(request)
    if isinstance(redir, RedirectResponse): return JSONResponse({"error":"not_logged"}, 401)
    if not user.get("x_init_data"):
        return JSONResponse({"valid": False, "reason": "no_xi"})
    load_user_session(user)
    if not api.is_ok():
        return JSONResponse({"valid": False, "reason": "reauth_failed"})
    d = api.api_init()
    if not d or "balance" not in d:
        return JSONResponse({"valid": False, "reason": "api_failed"})
    # Update cached names if changed
    if d.get("vkid"):
        auth_db.update_user(user["id"],
            vk_uid=d["vkid"],
            vk_first=d.get("first_name",""),
            vk_last=d.get("last_name",""),
        )
    return JSONResponse({
        "valid":   True,
        "vk_uid":  d.get("vkid"),
        "name":    f"{d.get('first_name','')} {d.get('last_name','')}".strip(),
        "balance": d.get("balance"),
        "income":  d.get("income"),
    })

@app.post("/api/profile/refresh-xi")
async def refresh_xi(request: Request):
    """Re-auth with existing xi — get fresh token."""
    redir, user = require_login(request)
    if isinstance(redir, RedirectResponse): return JSONResponse({"error":"not_logged"}, 401)
    if not user.get("x_init_data"):
        return JSONResponse({"error": "Нет x-init-data"})
    import urllib.parse as _up
    xi = user["x_init_data"]
    api.sess["x_init_data"]  = xi
    api.sess["check_string"] = _up.unquote(xi)
    api.sess["vk_user_id"]   = user.get("vk_uid") or 0
    if api.sess["vk_user_id"]:
        api.sess["x_version"] = api.int_to_base32(api.sess["vk_user_id"])
    api.sess["user_token"] = ""
    _token_cache.pop(user["id"], None)
    ok = api.reauth()
    if ok:
        _token_cache[user["id"]] = api.sess["user_token"]
        _profile_cache.pop(user["id"], None)
        auth_db.audit(user["id"], user["username"], "REFRESH_TOKEN")
        return JSONResponse({"ok": True})
    return JSONResponse({"error": "Переавторизация не удалась — нужен новый x-init-data"})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    r = require_auth(request)
    if r: return r
    redir, user = require_login(request)
    uid = user["id"]
    profile = get_cached_profile(uid)
    if profile is None:
        profile = api.api_init()
        if profile is None and user.get("x_init_data"):
            _token_cache.pop(uid, None)
            load_user_session(user)
            profile = api.api_init()
        if profile:
            set_cached_profile(uid, profile)
    return templates.TemplateResponse("dashboard.html", {
        "request":  request, "profile": profile,
        "sess":     make_sess(get_current_user(request) or {}), "stats": api.stats, "cfg": api.cfg,
        "auto_on":  scheduler.running(),
        "logs":     list(reversed(api.event_log[-30:])),
        "fmt":      api.fmt, "fmt_date": api.fmt_date, "uptime": api.uptime(),
        "current_user": get_current_user(request),
    })

@app.get("/slaves", response_class=HTMLResponse)
async def slaves_page(request: Request):
    r = require_auth(request)
    if r: return r
    data   = api.api_my_slaves()
    slaves = data.get("slaves",[]) if isinstance(data,dict) else (data if isinstance(data,list) else [])
    slaves = sorted([s for s in slaves if s.get("vkid") != api.sess.get("vk_user_id")],
                    key=lambda x: x.get("cost",0), reverse=True)
    return templates.TemplateResponse("slaves.html", {
        "request": request, "slaves": slaves,
        "total_cost": sum(s.get("cost",0) for s in slaves),
        "total_inc":  sum(s.get("salary",s.get("income",0)) for s in slaves),
        "sess": make_sess(get_current_user(request) or {}), "fmt": api.fmt, "fmt_date": api.fmt_date, "owner_name": api.owner_name,
        "current_user": get_current_user(request),
    })

@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, vkid: str = ""):
    r = require_auth(request)
    if r: return r
    player = error = None
    slaves = []; total_slave_cost = total_slave_inc = 0
    if vkid and vkid.isdigit():
        profile = api.api_profile(int(vkid))
        if profile:
            player = profile.get("user", profile)
        else:
            error = f"Игрок id:{vkid} не найден"
    if player:
        sd = api.api_profile_slaves(int(vkid))
        if isinstance(sd, dict):   slaves = sd.get("slaves",[])
        elif isinstance(sd, list): slaves = sd
        slaves = sorted(slaves, key=lambda x: x.get("cost",0), reverse=True)
        total_slave_cost = sum(s.get("cost",0) for s in slaves)
        total_slave_inc  = sum(s.get("salary",s.get("income",0)) for s in slaves)
    return templates.TemplateResponse("search.html", {
        "request": request, "player": player, "vkid": vkid, "error": error,
        "slaves": slaves, "total_slave_cost": total_slave_cost, "total_slave_inc": total_slave_inc,
        "sess": make_sess(get_current_user(request) or {}), "fmt": api.fmt, "fmt_date": api.fmt_date,
        "owner_name": api.owner_name, "is_my": api.is_my_slave,
        "current_user": get_current_user(request),
    })

@app.get("/top", response_class=HTMLResponse)
async def top_page(request: Request):
    r = require_auth(request)
    if r: return r
    members  = api.api_top_members()
    king     = api.api_top_king()
    position = api.api_top_position()
    players  = members if isinstance(members,list) else []
    return templates.TemplateResponse("top.html", {
        "request": request, "players": players[:100], "king": king,
        "position": position.get("position") if isinstance(position,dict) else None,
        "sess": make_sess(get_current_user(request) or {}), "fmt": api.fmt, "fmt_date": api.fmt_date,
        "current_user": get_current_user(request),
    })

@app.get("/scanner", response_class=HTMLResponse)
async def scanner_page(request: Request):
    r = require_auth(request)
    if r: return r
    return templates.TemplateResponse("scanner.html", {
        "request": request, "state": scanner_state, "cfg": api.cfg,
        "fmt": api.fmt, "sess": make_sess(get_current_user(request) or {}), "stats": api.stats, "auto_on": scheduler.running(),
        "current_user": get_current_user(request),
    })

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    r = require_auth(request)
    if r: return r
    return templates.TemplateResponse("settings.html", {
        "request": request, "cfg": api.cfg, "sess": make_sess(get_current_user(request) or {}),
        "stats": api.stats, "auto_on": scheduler.running(),
        "fmt": api.fmt, "uptime": api.uptime(),
        "current_user": get_current_user(request),
    })

# ══════════════════════════════════════════════════════
#  API ENDPOINTS
# ══════════════════════════════════════════════════════

def _chk(request):
    """Auth check + load caller's VK session."""
    r = require_auth(request)
    if r: return r
    _, user = require_login(request)
    if user and user.get("x_init_data"):
        load_user_session(user)
    return None

@app.post("/api/revenue")
async def do_revenue(request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    data = api.api_revenue()
    if not data: return JSONResponse({"error":"no_response"})
    added = data.get("add", data.get("coins_earned",0)) or 0
    if added > 0:
        api.stats["revenue_collected"] += int(added)
        api.add_log(f"Собрано {api.fmt(int(added))} монет", "ok")
    return JSONResponse({"ok":True,"added":added,"message":data.get("message","")})

@app.post("/api/bonus")
async def do_bonus(request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    data = api.api_daily_bonus()
    if not data: return JSONResponse({"error":"no_response"})
    msg = data.get("message","")
    already = "уже" in msg.lower() or "already" in msg.lower()
    if not already:
        api.stats["bonus_collected"] += 1
        api.add_log("Ежедневный бонус получен","ok")
    return JSONResponse({"ok":True,"message":msg,"already":already})

@app.post("/api/buy/{vkid}")
async def do_buy(vkid: int, request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    profile = api.api_profile(vkid)
    if not profile: return JSONResponse({"error":"Профиль не найден"})
    u = profile.get("user", profile)
    cost = u.get("cost", u.get("sale_price",0))
    if not cost or cost <= 0: return JSONResponse({"error":"Цена неизвестна"})
    result = api.api_buy(vkid, cost)
    if not result: return JSONResponse({"error":"Нет ответа"})
    err = result.get("error") or result.get("message")
    if err: return JSONResponse({"error":err})
    api.stats["bought_count"] += 1
    api.stats["bought_total"] += cost
    name = f"{u.get('first_name','')} {u.get('last_name','')}".strip()
    api.add_log(f"Куплен: {name} за {api.fmt(cost)} монет","ok")
    redir2, cur_user = require_login(request)
    if not redir2 and cur_user:
        auth_db.audit(cur_user["id"], cur_user["username"], "BUY",
            target=f"vkid:{vkid}", detail=f"{name} за {cost}")
        auth_db.trade_add(cur_user["id"], cur_user["username"], "buy",
            vk_id=vkid, vk_name=name, price=cost,
            balance_after=result.get("new_balance"))
    return JSONResponse({"ok":True,"cost":cost,"new_balance":result.get("new_balance")})

@app.post("/api/sell/{vkid}")
async def do_sell(vkid: int, request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    result = api.api_sell(vkid)
    if not result: return JSONResponse({"error":"Нет ответа"})
    err = result.get("error") or result.get("message")
    if err: return JSONResponse({"error":err})
    api.stats["sold_count"] += 1
    api.add_log(f"Продан раб id:{vkid}","info")
    redir2, cur_user = require_login(request)
    if not redir2 and cur_user:
        auth_db.audit(cur_user["id"], cur_user["username"], "SELL",
            target=f"vkid:{vkid}")
        auth_db.trade_add(cur_user["id"], cur_user["username"], "sell",
            vk_id=vkid, balance_after=result.get("new_balance"))
    return JSONResponse({"ok":True,"new_balance":result.get("new_balance")})

@app.post("/api/auto/toggle")
async def auto_toggle(request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    if scheduler.running():
        scheduler.stop(api); api.save_session()
        return JSONResponse({"running":False})
    else:
        scheduler.start(api); api.save_session()
        return JSONResponse({"running":True})

@app.post("/api/settings")
async def save_settings(request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    body = await request.json()
    for k,v,cast,mn in [("max_price",body.get("max_price"),int,0),
                         ("revenue_interval",body.get("revenue_interval"),int,30),
                         ("delay_buy",body.get("delay_buy"),float,0.3),
                         ("max_buy",body.get("max_buy"),int,1)]:
        if v is not None:
            try: api.cfg[k] = max(mn, cast(v))
            except: pass
    for k in ["auto_bonus","skip_clans"]:
        if k in body: api.cfg[k] = bool(body[k])
    if "targets" in body and isinstance(body["targets"],list):
        api.cfg["targets"] = [int(t) for t in body["targets"] if str(t).isdigit()]
    api.save_session()
    return JSONResponse({"ok":True})

@app.post("/api/reauth")
async def do_reauth(request: Request):
    if not api.sess["x_init_data"]: return JSONResponse({"error":"Нет x-init-data"})
    ok = api.reauth()
    return JSONResponse({"ok":ok})

@app.post("/api/settings/xi")
async def update_xi(request: Request):
    redir, user = require_login(request)
    if redir: return JSONResponse({"error":"not_logged"}, 401)
    import urllib.parse as _up
    body = await request.json()
    xi = body.get("x_init_data","").strip()
    if not xi or len(xi) < 30:
        return JSONResponse({"error":"Слишком короткое значение"})
    log.info(f"[xi] user={user.get('username')} updating xi len={len(xi)}")
    api.sess["x_init_data"]  = xi
    api.sess["check_string"] = _up.unquote(xi)
    api.sess["user_token"]   = ""
    _token_cache.pop(user["id"], None)
    result = api.api_auth()
    if not result or not api.sess.get("user_token"):
        log.warning(f"[xi] auth failed for user={user.get('username')}")
        return JSONResponse({"error":"Авторизация не удалась — проверь x-init-data"})
    d = api.api_init()
    vk_uid   = d.get("vkid")          if d else None
    vk_first = d.get("first_name","") if d else ""
    vk_last  = d.get("last_name","")  if d else ""
    if vk_uid:
        api.sess["vk_user_id"] = vk_uid
        api.sess["vk_first"]   = vk_first
        api.sess["vk_last"]    = vk_last
        api.sess["x_version"]  = api.int_to_base32(vk_uid)
    # SAVE TO DB — survives restarts
    auth_db.update_user(user["id"], x_init_data=xi, vk_uid=vk_uid, status="active")
    _token_cache[user["id"]] = api.sess["user_token"]
    _profile_cache.pop(user["id"], None)
    api.save_session()
    api.add_log(f"x-init-data обновлён: {vk_first} id:{vk_uid}", "ok")
    log.info(f"[xi] ok: user={user.get('username')} vk_uid={vk_uid}")
    return JSONResponse({"ok":True, "vk_uid":vk_uid, "name":f"{vk_first} {vk_last}".strip()})


@app.get("/api/logs")
async def get_logs(request: Request):
    redir, _ = require_login(request)
    if redir: return JSONResponse([])
    return JSONResponse(list(reversed(api.event_log[-50:])))

@app.get("/api/stats")
async def get_stats(request: Request):
    redir, _ = require_login(request)
    if redir: return JSONResponse({})
    return JSONResponse({
        "auto_on": scheduler.running(),
        "revenue_collected": api.stats["revenue_collected"],
        "bonus_collected":   api.stats["bonus_collected"],
        "bought_count":      api.stats["bought_count"],
        "bought_total":      api.stats["bought_total"],
        "sold_count":        api.stats["sold_count"],
        "errors":            api.stats["errors"],
        "uptime":            api.uptime(),
    })

# ══════════════════════════════════════════════════════
#  SCANNER
# ══════════════════════════════════════════════════════

def _scanner_worker(id_from,id_to,max_price,delay,auto_buy):
    scanner_state.update({"running":True,"start_time":datetime.now(),
                           "scanned":0,"found":[],"bought":0,"spent":0,"errors":0,"total":id_to-id_from+1})
    for vkid in range(id_from, id_to+1):
        if not scanner_state["running"]: break
        scanner_state["scanned"] += 1
        scanner_state["last_id"]  = vkid
        scanner_state["progress"] = round(scanner_state["scanned"]/scanner_state["total"]*100, 1)
        if scanner_state["scanned"] % 5 == 0 and _main_loop and _main_loop.is_running():
            asyncio.run_coroutine_threadsafe(_ws_broadcast(json.dumps({
                "type":"progress","progress":scanner_state["progress"],
                "scanned":scanner_state["scanned"],"last_id":vkid,
                "found":len(scanner_state["found"]),"bought":scanner_state["bought"],
                "errors":scanner_state["errors"],"running":True,
            })), _main_loop)
        try:
            if vkid == api.sess.get("vk_user_id"):
                time.sleep(delay); continue
            profile = api.api_profile(vkid)
            if not profile or not isinstance(profile,dict):
                scanner_state["errors"] += 1; time.sleep(delay); continue
            if profile.get("error") or profile.get("message"):
                scanner_state["errors"] += 1; time.sleep(delay); continue
            u = profile.get("user", profile)
            if not u.get("first_name"):
                time.sleep(delay); continue
            cost      = u.get("cost", 0)
            owner_raw = u.get("owner") or u.get("master_id")
            if not isinstance(cost,(int,float)) or cost<=0 or cost>max_price: time.sleep(delay); continue
            if api.is_my_slave(owner_raw): time.sleep(delay); continue
            if api.cfg.get("skip_clans") and (u.get("group") or u.get("squad")): time.sleep(delay); continue
            name  = f"{u['first_name']} {u.get('last_name','')}".strip()
            entry = {"vkid":vkid,"name":name,"cost":int(cost),
                     "income":u.get("income",u.get("salary",0)) or 0,
                     "owner":api.owner_name(owner_raw),"bought":False}
            scanner_state["found"].append(entry)
            api.add_log(f"Найден: {name} [{vkid}] за {api.fmt(int(cost))}","ok")
            if _main_loop and _main_loop.is_running():
                asyncio.run_coroutine_threadsafe(_ws_broadcast(json.dumps({
                    "type":"found","entry":entry,"progress":scanner_state["progress"],
                    "scanned":scanner_state["scanned"],"found":len(scanner_state["found"]),
                })), _main_loop)
            if auto_buy:
                result = api.api_buy(vkid, int(cost))
                if result and not (result.get("error") or result.get("message")):
                    entry["bought"] = True
                    scanner_state["bought"] += 1; scanner_state["spent"] += int(cost)
                    api.stats["bought_count"] += 1; api.stats["bought_total"] += int(cost)
        except Exception as e:
            scanner_state["errors"] += 1
            api.add_log(f"Сканер: исключение на id:{vkid}: {e}","err")
        time.sleep(delay)
    scanner_state["running"]  = False
    scanner_state["progress"] = 100 if scanner_state["scanned"] >= scanner_state["total"] else scanner_state["progress"]
    api.add_log(f"Сканер: проверено {scanner_state['scanned']}, найдено {len(scanner_state['found'])}, куплено {scanner_state['bought']}","info")
    if _main_loop and _main_loop.is_running():
        asyncio.run_coroutine_threadsafe(_ws_broadcast(json.dumps({"type":"done","state":_safe_state()})), _main_loop)

async def _ws_broadcast(msg):
    dead = set()
    for ws in scanner_ws_clients.copy():
        try: await ws.send_text(msg)
        except: dead.add(ws)
    scanner_ws_clients -= dead

def _safe_state():
    s = dict(scanner_state)
    s["start_time"] = s["start_time"].isoformat() if s["start_time"] else None
    return s

@app.post("/api/scanner/start")
async def scanner_start(request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    if scanner_state["running"]: return JSONResponse({"error":"Уже запущен"})
    redir2, cur_user = require_login(request)
    body = await request.json()
    id_from   = int(body.get("id_from",1))
    id_to     = int(body.get("id_to",1000))
    max_price = int(body.get("max_price", api.cfg["max_price"]))
    delay     = float(body.get("delay",0.8))
    auto_buy  = bool(body.get("auto_buy",False))
    if id_from >= id_to: return JSONResponse({"error":"id_from должен быть меньше id_to"})
    scanner_state["started_by"] = cur_user["id"] if cur_user else None
    threading.Thread(target=_scanner_worker,args=(id_from,id_to,max_price,delay,auto_buy),daemon=True).start()
    return JSONResponse({"ok":True})

@app.post("/api/scanner/stop")
async def scanner_stop(request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    redir2, cur_user = require_login(request)
    # Only the user who started OR admin can stop
    if cur_user and cur_user.get("role") != "admin":
        started_by = scanner_state.get("started_by")
        if started_by and started_by != cur_user["id"]:
            return JSONResponse({"error":"Не твой сканер"}, 403)
    scanner_state["running"] = False
    return JSONResponse({"ok":True})

@app.post("/api/scanner/buy/{vkid}")
async def scanner_buy(vkid: int, request: Request):
    if _chk(request): return JSONResponse({"error":"not_auth"},401)
    entry = next((e for e in scanner_state["found"] if e["vkid"]==vkid), None)
    if not entry: return JSONResponse({"error":"Не найден"})
    if entry["bought"]: return JSONResponse({"error":"Уже куплен"})
    result = api.api_buy(vkid, entry["cost"])
    if not result: return JSONResponse({"error":"Нет ответа"})
    err = result.get("error") or result.get("message")
    if err: return JSONResponse({"error":err})
    entry["bought"] = True
    scanner_state["bought"] += 1; scanner_state["spent"] += entry["cost"]
    api.stats["bought_count"] += 1; api.stats["bought_total"] += entry["cost"]
    api.add_log(f"Куплен из сканера: {entry['name']} за {api.fmt(entry['cost'])}","ok")
    return JSONResponse({"ok":True,"new_balance":result.get("new_balance")})

@app.get("/api/scanner/state")
async def scanner_get_state(request: Request):
    redir, _ = require_login(request)
    if redir: return JSONResponse({})
    return JSONResponse(_safe_state())

@app.websocket("/ws/scanner")
async def scanner_ws(websocket: WebSocket):
    await websocket.accept()
    scanner_ws_clients.add(websocket)
    try:
        await websocket.send_text(json.dumps({"type":"state","state":_safe_state()}))
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        scanner_ws_clients.discard(websocket)

@app.get("/api/debug/profile/{vkid}")
async def debug_profile(vkid: int, request: Request):
    redir, _ = require_admin(request)
    if redir: return JSONResponse({"error":"forbidden"},403)
    return JSONResponse(api.api_profile(vkid))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
def make_sess(user: dict) -> dict:
    """Build sess context that base.html needs: panel_role, vk_first, vk_last, vk_user_id."""
    base = dict(api.sess) if api.sess else {}
    base["panel_role"]  = user.get("role", "user")
    base["panel_user"]  = user.get("username", "")
    # Only override vk_first/last if api.sess is empty (admin without VK)
    if not base.get("vk_first"):
        base["vk_first"] = user.get("username", "")
        base["vk_last"]  = ""
    if not base.get("vk_user_id"):
        base["vk_user_id"] = user.get("vk_uid", "")
    return base