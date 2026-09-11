"""Kết nối Microsoft Fabric — thay p1_extract.py khi máy không có file .pbix cục bộ.

Xác thực DELEGATED bằng device code flow (msal) — mỗi người tự đăng nhập bằng chính
tài khoản Power BI Pro của họ, KHÔNG dùng service principal. Lý do chọn hướng này và
toàn bộ quyết định đã chốt: xem BAN GIAO - Analytics Engine App.md mục 7a.

Kéo đúng các bảng dim/fact mà p1_extract.py đọc từ PBIX bằng PBIXRay, nhưng qua REST
API executeQueries của Power BI Service. Ghi ra CÙNG TÊN FILE trong data/ (dim_*.parquet,
fact_*.parquet) để p2_prep.py chạy tiếp KHÔNG cần sửa gì — không làm lại việc lọc VCX/
gộp CGQ-DGQ/chuẩn hoá chiều ở đây, đúng nguyên tắc "công thức có một nguồn duy nhất".

Hai điều CHƯA XÁC MINH được cho tới khi chạy thật (xem BAN GIAO mục 7a):
  1. Bảng publish lên Fabric có đúng tên/cột như PBIX gốc không.
  2. executeQueries giới hạn bao nhiêu dòng/lần gọi.
--probe kiểm tra cả hai trước khi chạy full extract.

CLI:
  py -3.13 fabric_extract.py --login    đăng nhập (in mã + link ra terminal, chờ tới khi xong)
  py -3.13 fabric_extract.py --whoami   xem đang đăng nhập bằng tài khoản nào
  py -3.13 fabric_extract.py --probe    kiểm tra kết nối + schema + số dòng, KHÔNG ghi file
  py -3.13 fabric_extract.py --logout   xoá phiên đăng nhập đã lưu
  py -3.13 fabric_extract.py            chạy trích xuất đầy đủ (cần đã --login trước)
"""
import argparse
import json
import os
import sys
import threading
import time
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests
import msal
import contextvars

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # console mặc định cp1252 trên Windows vỡ với tiếng Việt có dấu

# Bản vendor cho app/ (đóng gói vào standalone) — hai loại đường dẫn KHÁC NHAU khi chạy dưới dạng .exe
# đóng băng bằng PyInstaller:
#   _BUNDLE_DIR (sys._MEIPASS) — nơi các file đưa vào qua --add-data được giải nén ra, XOÁ SAU MỖI LẦN
#     CHẠY. fabric_config.json (4 ID, chỉ đọc, đóng gói sẵn) phải tìm ở đây.
#   HERE (thư mục chứa chính file .exe, qua sys.executable) — SỐNG QUA các lần mở lại. Token cache
#     (ghi ra sau khi đăng nhập) phải nằm ở đây, nếu không mỗi lần mở app lại phải đăng nhập lại từ đầu.
# Đã từng nhầm cả hai dùng chung HERE kiểu exe-adjacent, khiến fabric_config.json "biến mất" trong bản
# đóng gói dù đã --add-data đúng — vì file đó thực ra nằm ở _MEIPASS, không nằm cạnh .exe.
_FROZEN = getattr(sys, "frozen", False)
_BUNDLE_DIR = sys._MEIPASS if _FROZEN else os.path.dirname(os.path.abspath(__file__))  # noqa: SLF001
HERE = os.path.dirname(os.path.abspath(sys.executable)) if _FROZEN else os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(_BUNDLE_DIR, "fabric_config.json")
OUT = os.path.join(HERE, "data")

AUTHORITY_TMPL = "https://login.microsoftonline.com/{tenant}"
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]
TOKEN_CACHE_FILE = os.path.join(HERE, "fabric_token_cache.bin")
# Token của phiên web hiện tại (server.py đặt trước mỗi request). Có giá trị thì run_dax dùng nó thay
# cho token cache file dùng chung, nhờ vậy mỗi người dùng web truy vấn Fabric bằng đúng tài khoản mình.
SESSION_TOKEN = contextvars.ContextVar("fabric_session_token", default=None)

# Ngưỡng khởi điểm để quyết định có cần chia nhỏ hay không — KHÔNG phải giới hạn cứng: bảng nhiều
# cột (fact_policy/CGQ/DGQ ~30-35 cột) đã đo thực tế: 9-20 nghìn dòng/lần thường lọt, 33 nghìn trở lên
# gần như chắc vỡ giới hạn byte phản hồi (~15MB, lỗi DaxByteCountNotSupported). Đặt 20.000 để giảm số
# lượt chia thừa (mỗi lần vỡ byte tốn thêm một lượt gọi COUNTROWS + một lượt pull thất bại trước khi
# chia tiếp) — _pull_range() vẫn tự chia đôi theo ngày, rồi theo Mã NV, nếu ngưỡng này vẫn chưa đủ.
MAX_ROWS_PER_QUERY = 20000


def log(msg):
    print(msg, flush=True)


# ============================================================== theo dõi tiến độ — server.py polling
# để vẽ progress bar (thay vì đổ nguyên log thô ra một hộp đen). Cập nhật ở đúng những điểm THỰC SỰ có
# dữ liệu vừa kéo về (không suy đoán %), tốc độ dòng/giây do phía frontend tự tính từ rowsSoFar/thời gian.
PROGRESS_LOCK = threading.Lock()
PROGRESS = {"phase": "", "table": "", "rowsSoFar": 0, "startedAt": None, "done": True, "error": None}


def _progress_reset():
    with PROGRESS_LOCK:
        PROGRESS.update(phase="Đang bắt đầu…", table="", rowsSoFar=0, startedAt=time.time(), done=False, error=None)


def _progress_set(phase=None, table=None):
    with PROGRESS_LOCK:
        if phase is not None:
            PROGRESS["phase"] = phase
        if table is not None:
            PROGRESS["table"] = table


def _progress_add_rows(n):
    with PROGRESS_LOCK:
        PROGRESS["rowsSoFar"] += n


def _progress_done(error=None):
    with PROGRESS_LOCK:
        PROGRESS["done"] = True
        PROGRESS["error"] = error


def progress_snapshot():
    with PROGRESS_LOCK:
        return dict(PROGRESS)


def load_config():
    # Ưu tiên file cạnh .exe thật (HERE) nếu ai đó đặt riêng để đổi 4 ID mà không cần build lại — rơi
    # về bản đóng gói sẵn (CONFIG_FILE, trong _BUNDLE_DIR) nếu không có.
    env_ids = {k: os.environ.get("FABRIC_" + k.upper()) for k in ("tenantId", "clientId", "workspaceId", "datasetId")}
    if all(env_ids.values()):
        return env_ids
    override = os.path.join(HERE, "fabric_config.json")
    path = override if os.path.isfile(override) else CONFIG_FILE
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Chưa có fabric_config.json cạnh fabric_extract.py — cần Tenant ID/Client ID/"
            "Workspace ID/Dataset ID từ IT (xem BAN GIAO mục 7a, Phần A).")
    return json.load(open(path, encoding="utf-8"))


# ============================================================== token cache
# ĐÃ THỬ keyring (Windows Credential Manager) và BỎ: Credential Manager giới hạn ~2,5KB mỗi mục,
# còn cache MSAL thật (kèm id_token/access_token/refresh_token) ra khoảng 8KB — keyring.set_password
# âm thầm lỗi (CredWrite thất bại), rơi về file, nhưng lượt đọc sau lại ưu tiên keyring (rỗng) nên
# mất luôn phiên đăng nhập. Dùng thẳng file cục bộ, cùng mức rủi ro đã chấp nhận với sync_token.txt
# (đọc được nếu ai đó có quyền truy cập máy này, nhưng không rời khỏi máy và đã gitignore).

def _load_cache():
    cache = msal.SerializableTokenCache()
    if os.path.isfile(TOKEN_CACHE_FILE):
        cache.deserialize(open(TOKEN_CACHE_FILE, "r", encoding="utf-8").read())
    return cache, "file"


def _save_cache(cache, backend):
    if not cache.has_state_changed:
        return
    with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
        f.write(cache.serialize())


def get_app():
    cfg = load_config()
    cache, backend = _load_cache()
    app = msal.PublicClientApplication(
        cfg["clientId"], authority=AUTHORITY_TMPL.format(tenant=cfg["tenantId"]), token_cache=cache)
    return app, cache, backend


def get_cached_token():
    """Token từ phiên đăng nhập trước, không cần tương tác lại. None nếu chưa từng đăng nhập / hết hạn hẳn."""
    app, cache, backend = get_app()
    accounts = app.get_accounts()
    if not accounts:
        return None
    result = app.acquire_token_silent(SCOPES, account=accounts[0])
    _save_cache(cache, backend)
    return result["access_token"] if result and "access_token" in result else None


def login_device_start():
    """Bước 1/2 của device code flow — gọi ngay, trả về code+link để người dùng nhập trên trình duyệt.
    Trả (app, flow, cache, backend) — cần cả bốn để gọi login_device_wait ngay sau đó."""
    app, cache, backend = get_app()
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        raise RuntimeError(f"Không khởi tạo được đăng nhập: {flow.get('error_description', flow)}")
    return app, flow, cache, backend


def login_device_wait(app, flow, cache, backend):
    """Bước 2/2 — CHẶN (blocking) cho tới khi người dùng hoàn tất trên trình duyệt, hoặc hết hạn
    (mặc định AAD ~15 phút). An toàn để chạy trong một thread riêng của server đa luồng."""
    result = app.acquire_token_by_device_flow(flow)
    _save_cache(cache, backend)
    if "access_token" not in result:
        raise RuntimeError(f"Đăng nhập thất bại: {result.get('error_description', result)}")
    return result["access_token"]


def logout():
    app, cache, backend = get_app()
    for acc in app.get_accounts():
        app.remove_account(acc)
    _save_cache(cache, backend)
    if backend == "file" and os.path.isfile(TOKEN_CACHE_FILE):
        os.remove(TOKEN_CACHE_FILE)


def whoami():
    app, _cache, _backend = get_app()
    accounts = app.get_accounts()
    return accounts[0]["username"] if accounts else None


# ============================================================== DAX execution

def _ident_table(name):
    return "'" + name.replace("'", "''") + "'"


def _ident_col(table, col):
    return f"{_ident_table(table)}[{col}]"


def _dax_str(v):
    """Chuỗi literal an toàn cho DAX — nhân đôi dấu ngoặc kép nhúng bên trong, đúng quy tắc escape của DAX."""
    return '"' + str(v).replace('"', '""') + '"'


def run_dax(query, token=None, retries=4):
    """Vài trăm lượt gọi liên tiếp trong một lượt trích xuất đầy đủ nên lỗi tạm thời (429 quá tải,
    5xx của service) là chuyện bình thường, không phải lỗi thật — tự thử lại có backoff trước khi
    báo lỗi. Lỗi DAX thật (400, cú pháp/tên cột sai) thì báo ngay, không thử lại vô ích."""
    token = token or SESSION_TOKEN.get() or get_cached_token()
    if not token:
        raise RuntimeError("Chưa đăng nhập Fabric hoặc phiên đã hết hạn — chạy lại luồng đăng nhập.")
    cfg = load_config()
    url = (f"https://api.powerbi.com/v1.0/myorg/groups/{cfg['workspaceId']}"
           f"/datasets/{cfg['datasetId']}/executeQueries")
    body = {"queries": [{"query": query}], "serializerSettings": {"includeNulls": True}}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=180)
        except requests.RequestException as e:
            last_err = e
            wait_s = 2 ** attempt
            log(f"  (mang loi, thu lai sau {wait_s}s — lan {attempt+1}/{retries}: {e})")
            time.sleep(wait_s)
            continue
        if r.status_code == 429 or r.status_code >= 500:
            last_err = RuntimeError(f"Fabric API lỗi tạm thời HTTP {r.status_code}: {r.text[:300]}")
            wait_s = min(30, 2 ** attempt + 1)
            log(f"  (Fabric qua tai HTTP {r.status_code}, thu lai sau {wait_s}s — lan {attempt+1}/{retries})")
            time.sleep(wait_s)
            continue
        if r.status_code != 200:
            raise RuntimeError(f"Fabric API lỗi HTTP {r.status_code}: {r.text[:1000]}")
        data = r.json()
        err = data.get("results", [{}])[0].get("error") if data.get("results") else data.get("error")
        if err:
            raise RuntimeError(f"DAX lỗi: {err}")
        return data["results"][0]["tables"][0]["rows"]
    raise RuntimeError(f"Fabric API lỗi liên tục sau {retries} lần thử: {last_err}")


def count_rows(table, filter_expr=None):
    src = _ident_table(table) if not filter_expr else f"FILTER({_ident_table(table)}, {filter_expr})"
    rows = run_dax(f'EVALUATE ROW("n", COUNTROWS({src}))')
    v = list(rows[0].values())[0]  # COUNTROWS tren mot doan rong tra ve BLANK -> JSON null, khong phai 0
    return int(v) if v is not None else 0


def pull_columns(table, cols, filter_expr=None, retry_unknown=True):
    """SELECTCOLUMNS(bảng [lọc filter_expr], "colA", bảng[colA], ...) -> DataFrame đúng tên cols.
    Cột không tồn tại trên bản Fabric (schema publish có thể khác PBIX gốc) bị tự động bỏ và thử lại,
    thay vì làm cả lượt trích xuất gãy vì một cột lẻ."""
    remaining = list(cols)
    while True:
        src = _ident_table(table) if not filter_expr else f"FILTER({_ident_table(table)}, {filter_expr})"
        args = ", ".join(f'"{c}", {_ident_col(table, c)}' for c in remaining)
        query = f"EVALUATE SELECTCOLUMNS({src}, {args})"
        try:
            rows = run_dax(query)
            break
        except RuntimeError as e:
            msg = str(e)
            # Power BI bọc tên cột lỗi trong "<oii>...</oii>" ("cannot be found or may not be used"),
            # KHÔNG theo cú pháp DAX 'Bảng'[Cột] — dò theo tên cột trần thay vì cú pháp tham chiếu.
            bad = next((c for c in remaining if c in msg), None)
            if retry_unknown and bad and len(remaining) > 1:
                log(f"  ! cot '{bad}' khong ton tai tren Fabric ({table}) — bo qua, thu lai")
                remaining.remove(bad)
                continue
            raise
    # Fabric bọc tên cột trả về trong ngoặc vuông ("[Tên cột]") dù SELECTCOLUMNS đặt alias không
    # ngoặc — PHẢI để pandas tự suy ra tên cột từ key thật của dict rồi mới bóc ngoặc, không được
    # ép columns=remaining lúc dựng DataFrame (làm vậy mọi ô thành NaN vì key không khớp).
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df.columns = [c[1:-1] if c.startswith("[") and c.endswith("]") else c for c in df.columns]
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return _coerce_dates(df[cols])


def _coerce_dates(df):
    """executeQueries trả ngày dạng chuỗi ISO ("2026-01-01T00:00:00") — PBIXRay (p1_extract.py) trả
    thẳng datetime64 nên p2_prep.py mặc định làm phép trừ ngày trực tiếp. Ép kiểu ở đây cho cột BẮT ĐẦU
    bằng "Ngày" (mọi cột ngày thật trong POL_COLS/CGQ_COLS/DGQ_COLS/SNAP_COLS đều đặt tên kiểu này) —
    PHẢI dùng startswith, không phải "in", để không dính nhầm "Số ngày tồn" (số nguyên, không phải ngày)."""
    for c in df.columns:
        if c.startswith("Ngày"):
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def pull_sample(table, cols, filter_expr=None, n=5):
    """Lấy N dòng đầu bằng TOPN ngay trong DAX — KHÔNG kéo cả bảng rồi cắt ở phía client
    (bảng cả triệu dòng sẽ vỡ giới hạn byte phản hồi chỉ để xem thử vài dòng)."""
    src = _ident_table(table) if not filter_expr else f"FILTER({_ident_table(table)}, {filter_expr})"
    args = ", ".join(f'"{c}", {_ident_col(table, c)}' for c in cols)
    rows = run_dax(f"EVALUATE TOPN({n}, SELECTCOLUMNS({src}, {args}))")
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df.columns = [c[1:-1] if c.startswith("[") and c.endswith("]") else c for c in df.columns]
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df[cols]


def _is_size_error(e):
    """executeQueries trả về đúng một lỗi kích cỡ phản hồi (DaxByteCountNotSupported, quan sát thực tế
    ~15MB/lần gọi) — KHÁC với vượt ngưỡng số dòng. Bảng nhiều cột (fact_policy ~30 cột) có thể vỡ giới
    hạn byte ở mức dòng thấp hơn nhiều so với MAX_ROWS_PER_QUERY, nên phải bắt lỗi này riêng để chia tiếp
    thay vì coi là lỗi thật."""
    return "ByteCount" in str(e)


def _distinct_values(table, col, filter_expr):
    src = f"FILTER({_ident_table(table)}, {filter_expr})"
    rows = run_dax(f'EVALUATE DISTINCT(SELECTCOLUMNS({src}, "v", {_ident_col(table, col)}))')
    return [list(r.values())[0] for r in rows]


# Thứ tự trục chia dự phòng khi một ngày duy nhất vẫn vượt giới hạn byte, tức không còn chia được
# theo ngày nữa. Cả hai cột đều có mặt trong POL_COLS/CGQ_COLS/DGQ_COLS nên dùng chung cho mọi bảng fact.
FALLBACK_SPLIT_DIMS = ["Mã NV", "Ma_DV"]


def _pull_by_categorical(table, cols, filter_expr, dims):
    """Thử lần lượt từng cột trong `dims` (vd Mã NV rồi tới Mã đơn vị): tách theo giá trị, nhánh nào vẫn
    quá lớn thì đệ quy sang cột tiếp theo trong danh sách. Đã gặp thật ở ngày 2025-11-11 (13.414 dòng vẫn
    vỡ 15MB) — hôm đó chỉ có một Mã NV hoạt động nên phải chia thêm theo Mã đơn vị mới lọt."""
    if not dims:
        raise RuntimeError(f"{table}: da het truc chia (ngay -> {' -> '.join(FALLBACK_SPLIT_DIMS)}) ma van "
                            "vuot gioi han byte — can giam bot cot trong POL_COLS/CGQ_COLS/DGQ_COLS.")
    dim_col, rest = dims[0], dims[1:]
    values = _distinct_values(table, dim_col, filter_expr)
    frames = []
    for v in values:
        sub_filter = f'{filter_expr} && {_ident_col(table, dim_col)} = {_dax_str(v)}'
        n = count_rows(table, sub_filter)
        if n == 0:
            continue
        if n <= MAX_ROWS_PER_QUERY:
            try:
                log(f"    chia theo {dim_col}={v}: {n:,} dong")
                res = pull_columns(table, cols, sub_filter)
                _progress_add_rows(len(res))
                frames.append(res)
                continue
            except RuntimeError as e:
                if not _is_size_error(e):
                    raise
        log(f"    {dim_col}={v}: {n:,} dong van vuot gioi han byte, chia tiep theo "
            f"{rest[0] if rest else '(het truc)'}")
        sub = _pull_by_categorical(table, cols, sub_filter, rest)
        if sub is not None:
            frames.append(sub)
    return pd.concat(frames, ignore_index=True) if frames else None


def _pull_range(table, cols, base_filter, date_col, start, end):
    """Kéo dữ liệu trong [start,end] (đối tượng date). Nếu vượt ngưỡng số dòng HOẶC vượt giới hạn byte
    phản hồi thì chia đôi theo ngày và đệ quy — tổng quát hơn chia cố định theo năm/tháng, tự thích ứng
    với bảng nào rộng cột (dễ vỡ giới hạn byte) hay bảng nào chỉ nhiều dòng (dễ vỡ giới hạn dòng).
    Hết chia được theo ngày (còn đúng 1 ngày) mà vẫn vỡ byte thì chia tiếp theo FALLBACK_SPLIT_DIMS."""
    dcol = _ident_col(table, date_col)
    expr = (f"{base_filter} && {dcol} >= DATE({start.year},{start.month},{start.day}) "
            f"&& {dcol} <= DATE({end.year},{end.month},{end.day})")
    n = count_rows(table, expr)
    if n == 0:
        return None
    too_wide = start >= end  # không chia nhỏ hơn 1 ngày được nữa
    if n <= MAX_ROWS_PER_QUERY:
        try:
            log(f"  {table} {start}..{end}: {n:,} dong")
            res = pull_columns(table, cols, expr)
            _progress_add_rows(len(res))
            return res
        except RuntimeError as e:
            if not _is_size_error(e):
                raise
            if too_wide:
                log(f"  {table} {start}: {n:,} dong nhung vuot gioi han byte ca khi chi con 1 ngay, "
                    f"chia tiep theo {FALLBACK_SPLIT_DIMS[0]}")
                return _pull_by_categorical(table, cols, expr, FALLBACK_SPLIT_DIMS)
            log(f"  {table} {start}..{end}: {n:,} dong nhung vuot gioi han byte phan hoi, chia doi theo ngay")
    else:
        log(f"  {table} {start}..{end}: {n:,} dong vuot nguong {MAX_ROWS_PER_QUERY:,}, chia doi theo ngay")
    if too_wide:
        # con duong duy nhat toi day la n > MAX_ROWS_PER_QUERY nhung khong loi byte — van thu qua truc du phong truoc.
        return _pull_by_categorical(table, cols, expr, FALLBACK_SPLIT_DIMS)
    mid = start + (end - start) // 2
    left = _pull_range(table, cols, base_filter, date_col, start, mid)
    right = _pull_range(table, cols, base_filter, date_col, mid + timedelta(days=1), end)
    frames = [f for f in (left, right) if f is not None]
    return pd.concat(frames, ignore_index=True) if frames else None


def pull_columns_chunked(table, cols, base_filter, date_col, years):
    start = date(min(years), 1, 1)
    end = date(max(years), 12, 31)
    result = _pull_range(table, cols, base_filter, date_col, start, end)
    return result if result is not None else pd.DataFrame(columns=cols)


# ============================================================== bản đồ bảng — khớp p1_extract.py

DIMS = {
    "loai_xe": ("Loại xe", ["Mã loại xe", "Tên loại xe", "F0", "F1", "F2", "FTNDS", "SORT_FTNDS"]),
    "gia_tri_xe": ("Giá trị xe", ["Giá trị xe", "STT"]),
    "dia_ban": ("Địa bàn hoạt động", ["Hai giá trị đầu biển số xe", "Địa bàn (theo mã biển số)",
                                      "Phân nhóm khu vực của địa bàn", "Phân nhóm rủi ro từng địa bàn",
                                      "Tỉnh/TP sau sáp nhập 07/2025 (tham chiếu)"]),
    "xe_phanloai": ("Phân loại hiệu xe hãng xe", ["Mã xe", "Hãng xe làm sạch", "Dòng xe", "Kiểu thân xe",
                                                  "Phân khúc", "Số chỗ ngồi", "Nhiên liệu"]),
    "hang_xe": ("Hãng xe", ["Mã hãng xe", "Tên hãng xe"]),
    "dai_ly": ("Mã đại lý", ["Mã đại lý", "Tên đại lý", "Nhóm đại lý", "Loại đại lý"]),
    "don_vi": ("Mã đơn vị", ["Mã đơn vị", "ĐƠN VỊ", "Khu vực", "Nhóm quy mô", "Nhóm khu vực"]),
    "kenh": ("Kênh khai thác", ["Kênh"]),
    "nguon_dv": ("Mã nguồn DV", ["Mã nguồn khai thác", "Tên nguồn khai thác", "Nhóm nguồn khai thác",
                                 "Kênh khai thác", "Tên tắt"]),
    "can_bo": ("Mã cán bộ", ["Mã cán bộ khai thác", "Tên cán bộ khai thác"]),
    "nhom_rr": ("Mã nhóm rủi ro", ["Mã nhóm rủi ro", "Phân nhóm rủi ro", "Nhóm rủi ro chính(F0)", "Nhóm rủi ro chi tiết (F1)"]),
    "gara": ("Gara sửa chữa", ["ID", "MASOTHUE", "MAQUANLY", "TENTHUONGGOI_TEN_TAT",
                               "TENDANGKYKD", "GARA", "Phân nhóm SH/GR", "BHANG"]),
    # --- bổ sung cho các chiều phân tích mới của app (Đào sâu chi tiết) ---
    "loai_xe2": ("Phân nhóm loại xe", ["Mã Loại xe", "MDSD", "Số chỗ (đến)", "Trọng tải kg (đến)"]),
    "dong_co": ("Động cơ", ["Mã động cơ", "Tên động cơ"]),
}


# Danh sách cột CHỦ ĐỘNG rút gọn so với p1_extract.py (bản PBIX cục bộ) — đã rà từng cột đối chiếu
# với mọi nơi dùng thật (p2_prep.py + toàn bộ p3-p20 báo cáo cũ), bỏ đúng những cột không ai dùng tới
# ở đâu cả. CHỈ áp dụng cho đường Fabric vì đây là nơi tốn thời gian theo số cột (giới hạn byte/lượt
# gọi API), không đụng đến p1_extract.py — file đó còn dùng chung cho dự án khác, không được đổi.
POL_COLS = [
    "Nguồn dữ liệu", "Mã NV", "Số hợp đồng", "Mã khách hàng", "Tên khách hàng",
    "Ngày hiệu lực hợp đồng", "Ngày kết thúc hiệu lực", "Ngày kế toán",
    "Phí BH phân bổ", "Số tiền bảo hiểm của đối tượng bảo hiểm", "Giá trị của đối tượng bảo hiểm",
    "Hoa hồng BH gốc phân bổ",
    "Mã nhóm rủi ro", "Nhóm giá trị xe", "Mã xe", "Mã hãng xe", "Hiệu xe",
    "Hai giá trị đầu biển số xe",
    "Kênh khai thác", "Mã đại lý", "Mã cán bộ khai thác", "Mã nguồn khai thác",
    "Ma_DV",
    "Loại Khách Hàng", "Mục đích sử dụng", "Nhóm Thời gian sử dụng xe", "Thời gian sử dụng xe",
    "Năm sản xuất",
    "Tên Phòng ban", "1. Mã loại xe",
]
CGQ_COLS = [
    "Mã NV", "Số hồ sơ", "Số hợp đồng", "Mã khách hàng", "Tên khách hàng",
    "Ngày xảy ra tổn thất", "Ngày thông báo", "Ngày mở HSBT", "Ngày hiệu lực hợp đồng",
    "Số tiền tổn thất phân bổ", "Phí giám định phân bổ",
    "C_BOI_THUONG_THU_DOI_TBH", "Số ngày tồn", "Trạng thái HSBT",
    "Nguyên nhân tổn thất", "Sự kiện", "Mã nhóm rủi ro", "Nhóm giá trị xe", "Mã xe", "Mã hãng xe",
    "Hiệu xe", "Hai giá trị đầu biển số xe", "Kênh khai thác", "Mã đại lý", "Mã cán bộ khai thác",
    "Mã cán bộ bồi thường", "Mã nguồn khai thác", "Ma_DV", "Mã đơn vị xử lý",
    "Gara sửa chữa", "Tên Gara sửa chữa",
    "Chi phí Sơn", "Chi phí nhân công", "Phí phụ tùng",
    "Mã nguyên nhân chính", "Nguyên nhân chính", "Nguyên nhân phụ",
    "Phân loại số tiền tổn thất", "Mã động cơ",
]
DGQ_COLS = [
    "Mã NV", "Số hồ sơ", "Số hợp đồng", "Mã khách hàng", "Tên khách hàng",
    "Ngày xảy ra tổn thất", "Ngày thông báo", "Ngày mở HSBT", "Ngày giải quyết",
    "Ngày hiệu lực hợp đồng",
    "Số tiền bồi thường phân bổ", "Phí giám định phân bổ",
    "C_BOI_THUONG_THU_DOI_TBH",
    "Nguyên nhân tổn thất", "Sự kiện", "Mã nhóm rủi ro", "Nhóm giá trị xe", "Mã xe", "Mã hãng xe",
    "Hiệu xe", "Hai giá trị đầu biển số xe", "Kênh khai thác", "Mã đại lý", "Mã cán bộ khai thác",
    "Mã cán bộ bồi thường", "Mã nguồn khai thác", "Ma_DV", "Mã đơn vị xử lý",
    "Mã Gara sửa chữa", "Mã gara sửa chữa xử lý", "Tên Gara sửa chữa",
    "Chi phí Sơn", "Chi phí nhân công", "Phí phụ tùng",
    "Mã nguyên nhân chính", "Nguyên nhân chính", "Nguyên nhân phụ",
    "Trạng thái HSBT",
    "Phân loại số tiền tổn thất", "Mã động cơ",
]
SNAP_COLS = ["Mã NV", "Số hồ sơ", "Ngày mở HSBT", "Số tiền tổn thất phân bổ", "Phí giám định phân bổ",
             "Số tiền tổn thất", "Phí giám định", "C_BOI_THUONG_THU_DOI_TBH", "Ngày xảy ra tổn thất"]

YEARS = range(2019, 2028)


def _vcx_codes():
    nv = pull_columns("Mã nghiệp vụ", ["Mã nghiệp vụ", "Nghiệp vụ", "Nhóm nghiệp vụ"])
    nv = nv.drop_duplicates("Mã nghiệp vụ")
    vcx = sorted(set(nv.loc[nv["Nhóm nghiệp vụ"].astype(str) == "5.3. BH VCX ô tô", "Mã nghiệp vụ"].astype(str)))
    return nv, vcx


def probe():
    """Kiểm tra kết nối + schema + quy mô dữ liệu, KHÔNG ghi file. Chạy trước khi extract_all()
    để trả lời hai câu hỏi còn treo trong BAN GIAO mục 7a."""
    log("=== Kiem tra dang nhap ===")
    token = get_cached_token()
    if not token:
        raise RuntimeError("Chua dang nhap — chay --login truoc.")
    log(f"Da dang nhap: {whoami()}")

    log("\n=== Kiem tra bang 'Ma nghiep vu' ===")
    nv, vcx = _vcx_codes()
    log(f"Tong {len(nv):,} ma nghiep vu, VCX o to: {vcx}")
    if not vcx:
        log("  ! KHONG tim thay ma nao thuoc nhom '5.3. BH VCX o to' — kiem tra lai gia tri that "
            "trong cot 'Nhóm nghiệp vụ' tren Fabric (co the ten khac PBIX goc).")
        log(f"  Cac gia tri Nhom nghiep vu hien co: {sorted(nv['Nhóm nghiệp vụ'].astype(str).unique())[:20]}")
        return
    vcx_set = "{" + ", ".join(_dax_str(v) for v in vcx) + "}"

    log("\n=== Kiem tra 3 bang fact chinh (dem dong, khong keo du lieu) ===")
    for tbl, col in [("DT kế toán", "Ngày kế toán"), ("Bồi thường CGQ", "Ngày xảy ra tổn thất"),
                      ("Bồi thường DGQ", "Ngày xảy ra tổn thất")]:
        try:
            base = f"{_ident_col(tbl, 'Mã NV')} IN {vcx_set}"
            if tbl == "DT kế toán":
                base += f' && {_ident_col(tbl, "Nguồn dữ liệu")} = "DBV"'
            n = count_rows(tbl, base)
            log(f"  {tbl:20s}: {n:,} dong (loc VCX o to)"
                + ("  -> se can chia theo nam/thang" if n > MAX_ROWS_PER_QUERY else ""))
        except Exception as e:
            log(f"  {tbl:20s}: LOI — {e}")

    log("\n=== Kiem tra mau du lieu (5 dong dau, cot khoa) ===")
    for tbl, cols in [("DT kế toán", ["Số hợp đồng", "Ngày hiệu lực hợp đồng", "Phí BH phân bổ",
                                       "Mã nhóm rủi ro", "Nhóm giá trị xe", "Mã hãng xe"]),
                       ("Bồi thường CGQ", ["Số hồ sơ", "Số hợp đồng", "Ngày xảy ra tổn thất",
                                           "Số tiền tổn thất phân bổ", "Nguyên nhân tổn thất"])]:
        try:
            base = f"{_ident_col(tbl, 'Mã NV')} IN {vcx_set}"
            sample = pull_sample(tbl, cols, base, n=5)
            log(f"  {tbl}:\n{sample.to_string(index=False)}")
        except Exception as e:
            log(f"  {tbl}: LOI khi lay mau — {e}")

    log("\nProbe xong. Neu cot/bang o tren dung nhu ky vong thi chay 'py -3.13 fabric_extract.py' de trich xuat day du.")


# ============================================================== Live Connection: gọi thẳng DAX measure
# có sẵn trong PBIX (đã kiểm chứng khớp logic Workbook, xem run_dax) thay vì kéo dữ liệu thô về tính —
# đường kéo-thô-theo-kỳ đã thử và bỏ: bảng "DT kế toán" là bảng CHI TIẾT KẾ TOÁN (nhiều dòng bút toán
# dùng chung ngày hiệu lực hợp đồng), lọc theo cửa sổ ngày gần như không giảm được số dòng khớp (dù chỉ
# 1 ngày vẫn khớp ~50% toàn bộ lịch sử) nên kéo-theo-kỳ không nhanh hơn kéo hết. server.py gọi run_dax()
# trực tiếp cho chế độ Live — không cần hàm riêng ở đây.


def extract_all(resume=False):
    """Mặc định LUÔN kéo lại toàn bộ — đúng ý nghĩa "dữ liệu mới nhất" khi người dùng bấm nút trong app.
    resume=True (chỉ dùng khi tự chạy tay để vá lỗi giữa chừng) bỏ qua bảng fact nào đã có sẵn file
    parquet, tránh kéo lại hàng chục phút vì một bảng khác lỗi — mỗi bảng fact ghi file ngay khi xong,
    độc lập với các bảng còn lại nên bỏ qua an toàn."""
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    _progress_reset()
    try:
        log("=== Ma nghiep vu VCX o to ===")
        _progress_set(phase="Đang xác định mã nghiệp vụ VCX ô tô…")
        nv, vcx = _vcx_codes()
        nv.to_parquet(f"{OUT}/dim_nghiep_vu.parquet", index=False)
        if not vcx:
            raise RuntimeError("Khong tim thay ma nghiep vu VCX o to tren Fabric — chay --probe de kiem tra lai.")
        log(f"Ma nghiep vu VCX o to: {vcx}")
        vcx_set = "{" + ", ".join(_dax_str(v) for v in vcx) + "}"

        log("\n=== Dimensions ===")
        _progress_set(phase="Đang kéo các bảng chiều (dimensions)…")
        for key, (tbl, cols) in DIMS.items():
            _progress_set(table=tbl)
            try:
                df = pull_columns(tbl, cols)
                _progress_add_rows(len(df))
                df.to_parquet(f"{OUT}/dim_{key}.parquet", index=False)
                log(f"dim {key:12s} <- {tbl:28s} {len(df):>7,} dong")
            except Exception as e:
                log(f"dim {key} LOI: {e}")

        log("\n=== Fact: hop dong (DT ke toan) ===")
        _progress_set(phase="Đang kéo Hợp đồng (DT kế toán)…", table="DT kế toán")
        pol_file = f"{OUT}/fact_policy.parquet"
        if resume and os.path.isfile(pol_file):
            log(f"-> {pol_file} da co san, bo qua (dang --resume)")
        else:
            filt_pol = (f'{_ident_col("DT kế toán", "Nguồn dữ liệu")} = "DBV" '
                        f'&& {_ident_col("DT kế toán", "Mã NV")} IN {vcx_set}')
            pol = pull_columns_chunked("DT kế toán", POL_COLS, filt_pol, "Ngày kế toán", YEARS)
            log(f"-> DT ke toan (VCX o to, DBV): {len(pol):,} dong")
            pol.to_parquet(pol_file, index=False)

        log("\n=== Fact: boi thuong (CGQ/DGQ) ===")
        for name, tbl, cols in [("cgq", "Bồi thường CGQ", CGQ_COLS), ("dgq", "Bồi thường DGQ", DGQ_COLS)]:
            fact_file = f"{OUT}/fact_{name}.parquet"
            _progress_set(phase=f"Đang kéo Bồi thường ({tbl})…", table=tbl)
            if resume and os.path.isfile(fact_file):
                log(f"-> {fact_file} da co san, bo qua (dang --resume)")
                continue
            filt = f'{_ident_col(tbl, "Mã NV")} IN {vcx_set}'
            df = pull_columns_chunked(tbl, cols, filt, "Ngày xảy ra tổn thất", YEARS)
            log(f"-> {tbl} (VCX o to): {len(df):,} dong")
            df.to_parquet(fact_file, index=False)

        log("\n=== Snapshot 31/12/2025 (du phong) ===")
        _progress_set(phase="Đang kéo snapshot dự phòng (31/12/2025)…", table="BT CGQ 3112025")
        try:
            filt = f'{_ident_col("BT CGQ 3112025", "Mã NV")} IN {vcx_set}'
            s = pull_columns("BT CGQ 3112025", SNAP_COLS, filt)
            _progress_add_rows(len(s))
            s.to_parquet(f"{OUT}/fact_snap2025.parquet", index=False)
            log(f"BT CGQ 3112025 (VCX): {len(s):,} dong")
        except Exception as e:
            log(f"snapshot LOI: {e}")

        log(f"\nTONG THOI GIAN: {time.time()-t0:.0f}s")
        log("Da ghi xong dim_*.parquet + fact_*.parquet — chay p2_prep.py de hoan tat lam sach.")
        _progress_set(phase="Đã kéo xong dữ liệu Fabric, chờ làm sạch…", table="")
    except Exception as e:
        _progress_done(error=str(e))
        raise
    else:
        _progress_done()


# ============================================================== CLI
# --login-start / --login-wait tách rời hai bước của device flow qua hai lần gọi script khác nhau —
# dùng khi cần thấy mã+link ngay (lần gọi 1) rồi mới chờ hoàn tất ở lần gọi 2, thay vì bị chặn
# xuyên suốt trong --login. Flow lưu tạm ra fabric_login_flow.json (tự xoá sau khi dùng xong).
FLOW_FILE = os.path.join(HERE, "fabric_login_flow.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--login", action="store_true")
    ap.add_argument("--login-start", action="store_true")
    ap.add_argument("--login-wait", action="store_true")
    ap.add_argument("--whoami", action="store_true")
    ap.add_argument("--logout", action="store_true")
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--resume", action="store_true",
                     help="bo qua bang fact da co san file parquet — chi dung khi tu chay tay de va loi giua chung")
    args = ap.parse_args()

    if args.logout:
        logout()
        log("Da xoa phien dang nhap Fabric da luu.")
    elif args.whoami:
        acc = whoami()
        log(f"Dang dang nhap: {acc}" if acc else "Chua dang nhap.")
    elif args.login_start:
        _app, flow, _cache, _backend = login_device_start()
        json.dump(flow, open(FLOW_FILE, "w", encoding="utf-8"))
        log(flow["message"])
    elif args.login_wait:
        if not os.path.isfile(FLOW_FILE):
            raise SystemExit("Chua co --login-start truoc.")
        flow = json.load(open(FLOW_FILE, encoding="utf-8"))
        app, cache, backend = get_app()
        login_device_wait(app, flow, cache, backend)
        os.remove(FLOW_FILE)
        log(f"Dang nhap thanh cong: {whoami()}")
    elif args.login:
        app, flow, cache, backend = login_device_start()
        log(flow["message"])
        login_device_wait(app, flow, cache, backend)
        log(f"Dang nhap thanh cong: {whoami()}")
    elif args.probe:
        probe()
    else:
        extract_all(resume=args.resume)
