"""Phiên người dùng + đăng nhập Microsoft cho bản web của DBV Analytics Engine.

Mỗi trình duyệt có một phiên riêng (cookie dbv_sid), giữ token cache MSAL riêng và toàn bộ trạng thái
phân tích riêng (dữ liệu đã nạp, cache heatmap...). Token Fabric của phiên được đưa vào fabric_extract
qua ContextVar, nên mọi truy vấn DAX chạy dưới đúng tài khoản của người đang xem và Power BI tự áp RLS.

Hai cách đăng nhập, cùng ghi vào token cache của phiên:
  - Authorization-code + PKCE (chuẩn cho web): /auth/login -> Microsoft -> /auth/callback. Cần IT đăng
    ký redirect URI (xem hướng dẫn trong TIEN_DO_PHIEN_LAM_VIEC.md).
  - Device code (giữ lại để thử trên máy khi chưa có redirect URI): /api/fabric_login_start + _wait.
"""
import contextvars
import os
import secrets
import threading
import time
from http.cookies import SimpleCookie
from urllib.parse import urlencode

import msal

SESSION_COOKIE = "dbv_sid"
SESSION_IDLE_SECONDS = 12 * 3600
BASE_URL = os.environ.get("DBV_BASE_URL", "http://localhost:8787").rstrip("/")
HOSTED = bool(os.environ.get("DBV_BASE_URL"))
LOGIN_REQUIRED = os.environ.get("DBV_LOGIN_REQUIRED", "1" if HOSTED else "0") == "1"
REDIRECT_URI = BASE_URL + "/auth/callback"
COOKIE_SECURE = BASE_URL.startswith("https://")

_SESSIONS = {}
_LOCK = threading.Lock()
_CTX_SESSION = contextvars.ContextVar("dbv_session", default=None)


def _new_session():
    return {"sid": secrets.token_urlsafe(32), "created": time.time(), "last": time.time(),
            "cache": None, "account": None, "auth_flow": None, "device_flow": None, "scoped": {}}


def _purge_idle():
    cutoff = time.time() - SESSION_IDLE_SECONDS
    for sid in [k for k, v in _SESSIONS.items() if v["last"] < cutoff]:
        del _SESSIONS[sid]


def get_or_create(cookie_header):
    """Trả (session, is_new). Gọi ở đầu mỗi request; is_new=True thì handler phải gửi Set-Cookie."""
    sid = None
    if cookie_header:
        c = SimpleCookie()
        try:
            c.load(cookie_header)
            if SESSION_COOKIE in c:
                sid = c[SESSION_COOKIE].value
        except Exception:  # noqa: BLE001
            sid = None
    with _LOCK:
        _purge_idle()
        sess = _SESSIONS.get(sid) if sid else None
        if sess is None:
            sess = _new_session()
            _SESSIONS[sess["sid"]] = sess
            sess["last"] = time.time()
            return sess, True
        sess["last"] = time.time()
        return sess, False


def cookie_header_value(sess):
    parts = [f"{SESSION_COOKIE}={sess['sid']}", "Path=/", "HttpOnly", "SameSite=Lax",
             f"Max-Age={SESSION_IDLE_SECONDS}"]
    if COOKIE_SECURE:
        parts.append("Secure")
    return "; ".join(parts)


def bind(sess):
    """Gắn phiên vào request hiện tại (ContextVar theo thread của ThreadingHTTPServer)."""
    return _CTX_SESSION.set(sess)


def unbind(token):
    _CTX_SESSION.reset(token)


def current():
    return _CTX_SESSION.get()


class SessionScoped:
    """Dict có cùng tên/cách dùng như biến toàn cục cũ, nhưng trỏ tới dict riêng của phiên hiện tại.
    Ngoài phạm vi request (bản standalone, gọi từ CLI) rơi về một dict toàn cục như trước."""

    def __init__(self, name, factory=dict):
        self._name = name
        self._factory = factory
        self._global = factory()

    def _t(self):
        sess = _CTX_SESSION.get()
        if sess is None:
            return self._global
        d = sess["scoped"].get(self._name)
        if d is None:
            d = sess["scoped"][self._name] = self._factory()
        return d

    def __getitem__(self, k):
        return self._t()[k]

    def __setitem__(self, k, v):
        self._t()[k] = v

    def __delitem__(self, k):
        del self._t()[k]

    def __contains__(self, k):
        return k in self._t()

    def __iter__(self):
        return iter(self._t())

    def __len__(self):
        return len(self._t())

    def __bool__(self):
        return bool(self._t())

    def get(self, k, default=None):
        return self._t().get(k, default)

    def pop(self, k, *a):
        return self._t().pop(k, *a)

    def setdefault(self, k, v=None):
        return self._t().setdefault(k, v)

    def update(self, *a, **kw):
        self._t().update(*a, **kw)

    def clear(self):
        self._t().clear()

    def items(self):
        return self._t().items()

    def keys(self):
        return self._t().keys()

    def values(self):
        return self._t().values()


# ------------------------------------------------------------------ MSAL theo phiên

def _cfg(fabric):
    return fabric.load_config()


def _session_app(fabric, sess):
    cache = msal.SerializableTokenCache()
    if sess.get("cache"):
        cache.deserialize(sess["cache"])
    cfg = _cfg(fabric)
    app = msal.PublicClientApplication(
        cfg["clientId"], authority=fabric.AUTHORITY_TMPL.format(tenant=cfg["tenantId"]), token_cache=cache)
    return app, cache


def _save(sess, cache):
    if cache.has_state_changed:
        sess["cache"] = cache.serialize()


def account(fabric, sess):
    if not sess.get("cache"):
        return None
    app, _ = _session_app(fabric, sess)
    accs = app.get_accounts()
    return accs[0]["username"] if accs else None


def access_token(fabric, sess):
    """Token còn hạn của phiên (MSAL tự refresh nếu cần). None nếu chưa đăng nhập."""
    if not sess.get("cache"):
        return None
    app, cache = _session_app(fabric, sess)
    accs = app.get_accounts()
    if not accs:
        return None
    result = app.acquire_token_silent(fabric.SCOPES, account=accs[0])
    _save(sess, cache)
    return result["access_token"] if result and "access_token" in result else None


def logout(fabric, sess):
    if sess.get("cache"):
        app, cache = _session_app(fabric, sess)
        for acc in app.get_accounts():
            app.remove_account(acc)
    sess["cache"] = None
    sess["account"] = None
    sess["auth_flow"] = None
    sess["device_flow"] = None
    sess["scoped"] = {}


# ---- authorization-code + PKCE

def login_url(fabric, sess):
    app, cache = _session_app(fabric, sess)
    flow = app.initiate_auth_code_flow(fabric.SCOPES, redirect_uri=REDIRECT_URI)
    if "auth_uri" not in flow:
        raise RuntimeError(f"Không khởi tạo được đăng nhập: {flow.get('error_description', flow)}")
    sess["auth_flow"] = flow
    _save(sess, cache)
    return flow["auth_uri"]


def finish_login(fabric, sess, query_params):
    """query_params: dict {tên: giá trị} từ URL callback."""
    flow = sess.get("auth_flow")
    if not flow:
        raise RuntimeError("Phiên đăng nhập không còn (mở lại trang rồi bấm Đăng nhập lần nữa).")
    app, cache = _session_app(fabric, sess)
    result = app.acquire_token_by_auth_code_flow(flow, query_params)
    sess["auth_flow"] = None
    _save(sess, cache)
    if "access_token" not in result:
        raise RuntimeError(f"Đăng nhập thất bại: {result.get('error_description', result)}")
    sess["account"] = account(fabric, sess)
    return sess["account"]


# ---- device code (dự phòng cho localhost)

def device_start(fabric, sess):
    app, cache = _session_app(fabric, sess)
    flow = app.initiate_device_flow(scopes=fabric.SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Không khởi tạo được đăng nhập: {flow.get('error_description', flow)}")
    sess["device_flow"] = flow
    _save(sess, cache)
    return flow


def device_wait(fabric, sess):
    flow = sess.get("device_flow")
    if not flow:
        raise RuntimeError("Chưa gọi bước khởi tạo đăng nhập.")
    app, cache = _session_app(fabric, sess)
    try:
        result = app.acquire_token_by_device_flow(flow)
    finally:
        sess["device_flow"] = None
    _save(sess, cache)
    if "access_token" not in result:
        raise RuntimeError(f"Đăng nhập thất bại: {result.get('error_description', result)}")
    sess["account"] = account(fabric, sess)
    return sess["account"]


def error_redirect(msg):
    return "/?" + urlencode({"auth_error": msg})
