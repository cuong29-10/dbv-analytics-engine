"""Backend thật cho DBV Analytics Engine — chạy local, không có request mạng ra ngoài.

Dùng lại đúng pipeline/engine.py (công thức đã kiểm chứng bằng verify_engine.py) — không viết
lại công thức ở đây. Hai nguồn dữ liệu:
  - CSV do người dùng tải lên (đúng template ở Bước 1) -> làm sạch tối giản ngay trong file này.
  - Dữ liệu đã làm sạch của pipeline (pipeline/data/clean_policy.parquet + clean_claims.parquet)
    -> nạp thẳng, không làm lại việc lọc VCX / gộp CGQ-DGQ / chuẩn hoá chiều mà p1+p2 đã làm.

Chạy: py -3.13 server.py   (mở http://localhost:8787)
FABRIC_TENANTID/CLIENTID/WORKSPACEID/DATASETID (biến môi trường) ghi đè fabric_config.json nếu cần.
"""
import io
import json
import os
import re
import socket
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import numpy as np
import pandas as pd
from openpyxl import Workbook

import webauth as WA

# STANDALONE=True khi chạy dưới dạng .exe đóng gói bằng PyInstaller (cờ "frozen" do PyInstaller đặt).
# Hai bản chạy cùng một đường: nghe loopback, tự mở trình duyệt, đăng nhập Microsoft theo từng người.
# Khác nhau đúng ba chỗ: nơi đọc file đi kèm (_MEIPASS), nơi ghi cache dữ liệu (cạnh .exe), và phiên
# đăng nhập được giữ ra file để mở lại app không phải đăng nhập lại.
STANDALONE = getattr(sys, "frozen", False)
BASE_DIR = sys._MEIPASS if STANDALONE else os.path.dirname(os.path.abspath(__file__))  # noqa: SLF001
HERE = os.path.dirname(os.path.abspath(__file__))
PIPE_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "pipeline"))
# DATA_DIR: nơi đọc/ghi clean_policy/clean_claims.parquet. Ở bản dev là pipeline/data, sống ngay
# trong repo. Ở bản đóng gói .exe KHÔNG dùng được thư mục giải nén tạm (sys._MEIPASS) làm nơi GHI —
# nó bị xoá và dựng lại hoàn toàn mới mỗi lần mở lại .exe. Dùng thư mục cạnh chính file .exe
# (sys.executable, sống qua các lần mở lại — cùng nguyên tắc đã áp dụng cho fabric_token_cache.bin).
# Bản đóng gói KHÔNG nhúng sẵn dữ liệu: thư mục này chỉ sinh ra khi người dùng thật sự bấm "Kéo dữ
# liệu từ Microsoft Fabric về", nhờ vậy file gửi đi nhẹ và ai chỉ dùng kết nối trực tiếp thì không có
# dữ liệu nào nằm lại trên máy.
if STANDALONE:
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "data")
else:
    DATA_DIR = os.path.join(PIPE_DIR, "data")
UI_FILE = os.path.join(BASE_DIR, "ui.html")
if STANDALONE:
    WA.PERSIST_FILE = os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "fabric_token_cache.bin")
PORT = int(os.environ.get("PORT", "8787"))

# Ưu tiên bản engine.py nằm cạnh server.py (bản đóng gói luôn có sẵn) — nếu không có
# thì lấy bản sống trong pipeline/ (đúng luồng phát triển nội bộ, luôn mới nhất).
sys.path.insert(0, BASE_DIR)
try:
    import engine as EG  # noqa: E402
except ImportError:
    sys.path.insert(0, PIPE_DIR)
    import engine as EG  # noqa: E402  (lõi tính toán thật của dự án, xem pipeline/engine.py)

# Kết nối Microsoft Fabric — cùng mẫu ưu tiên nạp như engine.py: thử bản vendor cạnh server.py trước
# (standalone LUÔN có sẵn bản này, đã đóng gói kèm 4 ID + fabric_extract.py qua --add-data), rơi về
# bản sống trong pipeline/ nếu không có (đúng luồng phát triển nội bộ). Đăng nhập vẫn cần trình duyệt
# tương tác nên chỉ hữu ích khi ai đó thật sự demo trực tiếp, nhưng không có gì ngăn nó CHẠY ĐƯỢC ở
# bản standalone — người dùng đã chốt chấp nhận 4 ID này nằm trong file .exe gửi đi. Thiếu module
# (máy khác chưa cài msal, hoặc chưa có fabric_config.json) thì tắt tuỳ chọn này, không chặn cả app.
FABRIC = None
try:
    import fabric_extract as FABRIC  # noqa: E402
except ImportError:
    if not STANDALONE:
        sys.path.insert(0, PIPE_DIR)
        try:
            import fabric_extract as FABRIC  # noqa: E402
        except ImportError as e:
            print(f"[fabric] tuy chon Fabric tat vi khong nap duoc fabric_extract.py: {e}", flush=True)
    else:
        print("[fabric] tuy chon Fabric tat — thieu fabric_extract.py/fabric_config.json trong ban dong goi.",
              flush=True)

# p1_extract.py / p2_prep.py: trước đây gọi qua subprocess.run([sys.executable, "..."]) — CHỈ chạy được
# ở bản dev, vì sys.executable trong bản .exe đóng gói là chính file .exe đã đóng băng, không phải một
# python.exe thật, "python p1_extract.py" không hoạt động (và cwd=PIPE_DIR trong bản đóng gói cũng
# không trỏ tới đâu cả, vì pipeline/ không nằm trong file .exe). Giờ import trực tiếp và gọi hàm run()
# trong cùng tiến trình — chạy được ở cả hai bản, và là điều kiện để "Kéo dữ liệu mới nhất" từ Fabric
# hoạt động được trong bản đóng gói (trước đây bị chặn cứng, xem lịch sử h_fabric_pull).
P2_PREP = None
try:
    import p2_prep as P2_PREP  # noqa: E402
except ImportError:
    if not STANDALONE:
        sys.path.insert(0, PIPE_DIR)
        try:
            import p2_prep as P2_PREP  # noqa: E402
        except ImportError as e:
            print(f"[pipeline] khong nap duoc p2_prep.py: {e}", flush=True)
    else:
        print("[pipeline] khong nap duoc p2_prep.py trong ban dong goi.", flush=True)

# ------------------------------------------------------------ phiên
# Mỗi trình duyệt một phiên (cookie dbv_sid), đăng nhập bằng tài khoản Microsoft của chính người dùng
# (xem webauth.py). Các nguồn CSV/Cache không cần tài khoản; riêng nhóm route Fabric tự kiểm tra token
# của phiên qua fabric_token(), nên Power BI luôn áp đúng phân quyền dữ liệu của người đang đăng nhập.


STORE = WA.SessionScoped("store", lambda: {"pol": None, "clm": None, "source": None, "loadedAt": None,
                                           "lastValidate": None})

POLICY_REQUIRED = ["Số hợp đồng", "Ngày hiệu lực hợp đồng", "Ngày kết thúc hiệu lực", "Phí BH phân bổ"]
POLICY_OPTIONAL = ["Mã đơn vị", "Kênh khai thác", "Hãng xe", "Dải giá trị xe", "Mục đích sử dụng",
                    "Loại khách hàng", "Nhóm thời gian sử dụng xe", "Nhóm rủi ro địa bàn", "Loại bút toán",
                    "Hiệu xe", "Dòng xe", "Phân khúc", "Nhiên liệu", "Số chỗ ngồi"]
CLAIMS_REQUIRED = ["Số hồ sơ", "Số hợp đồng", "Ngày xảy ra tổn thất", "Số tiền tổn thất", "Trạng thái hồ sơ"]
CLAIMS_OPTIONAL = ["Nguyên nhân tổn thất", "Xưởng sửa chữa", "Mô hình xưởng",
                    "Phân nhóm xưởng", "Tên xưởng viết tắt"]

# id, nhãn, tên cột CSV (bảng sở hữu cột này — Hợp đồng trừ khi id nằm trong CLAIMS_NATIVE_DIMS), tên cột parquet pipeline
DIM_DEFS = [
    ("donvi", "Đơn vị", "Mã đơn vị", "DonVi"),
    ("hangxe", "Hãng xe", "Hãng xe", "HangXe"),
    ("kenh", "Kênh khai thác", "Kênh khai thác", "Kenh"),
    ("daigiatri", "Giá trị xe", "Giá trị xe", "DaiGiaTriXe"),
    ("mucdich", "Mục đích sử dụng xe", "Mục đích sử dụng xe", "MDSD2"),
    ("nhomrr", "Nhóm rủi ro địa bàn", "Nhóm rủi ro địa bàn", "NhomRR_DiaBan"),
    ("thoigiansd", "Nhóm thời gian sử dụng xe", "Nhóm thời gian sử dụng xe", "Nhóm Thời gian sử dụng xe"),
    ("hieuxe", "Hiệu xe", "Hiệu xe", "Hiệu xe"),
    ("dongxe", "Dòng xe", "Dòng xe", "DongXe"),
    ("phankhuc", "Phân khúc", "Phân khúc", "PhanKhuc"),
    ("nhienlieu", "Nhiên liệu", "Nhiên liệu", "NhienLieu"),
    ("sochongoi", "Số chỗ ngồi", "Số chỗ ngồi", "SoChoNgoi"),
    ("kieuthanxe", "Kiểu thân xe", "Kiểu thân xe", "KieuThanXe"),
    ("phongkd", "Phòng kinh doanh", "Phòng kinh doanh", "PhongKD"),
    ("canbokt", "Cán bộ kinh doanh", "Cán bộ kinh doanh", "CanBoKT"),
    ("doitac", "Đối tác", "Đối tác", "DoiTac"),
    ("diemban", "Điểm bán", "Điểm bán", "DiemBan"),
    ("sochoden", "Số chỗ", "Số chỗ", "SoChoDen"),
    ("trongtaikg", "Trọng tải kg (đến)", "Trọng tải kg (đến)", "TrongTaiKgDen"),
    ("diaban", "Địa bàn", "Địa bàn", "Tinh"),
    # claims-native: không có ở phía Hợp đồng (một policy không có claim thì không có xưởng sửa chữa,
    # không có mức độ tổn thất, không có động cơ ghi trên hồ sơ giám định)
    ("nhomgara", "Phân nhóm SH/GR sửa chữa", "Phân nhóm xưởng", "NhomGara"),
    ("garatt", "Gara sửa chữa", "Tên xưởng viết tắt", "GaraTenTat"),
    ("nhommucdo", "Nhóm mức độ tổn thất", "Nhóm mức độ tổn thất", "NhomMucDoTT"),
    ("dongco", "Động cơ", "Động cơ", "DongCo"),
    ("loaixe_f0", "Nhóm xe cấp 1 (F0)", "Nhóm xe cấp 1 (F0)", "NhomXe_F0"),
    ("loaixe_f1", "Nhóm xe cấp 2 (F1)", "Nhóm xe cấp 2 (F1)", "NhomXe_F1"),
    ("loaixe_f2", "Nhóm xe cấp 3 (F2)", "Nhóm xe cấp 3 (F2)", "MucDichSD"),
    ("ftnds", "Phân loại TNDS (FTNDS)", "Phân loại TNDS (FTNDS)", "FTNDS"),
]
CLAIMS_NATIVE_DIMS = {"nhomgara", "garatt", "nhommucdo", "dongco"}
METRIC_KEYS = {"lr": "lr", "freq": "freq", "sev": "sev", "pp": "pp", "avgprem": "avgprem", "ae": "ae"}


# ============================================================== thứ tự hiển thị giá trị chiều
# Một số chiều có nhãn đánh sẵn số thứ tự nghiệp vụ (VD "1. Dưới 400tr", "II. XE CHỞ HÀNG") —
# khi TUYỆT ĐA SỐ giá trị của chiều đó khớp mẫu, sắp theo số/số La Mã đó thay vì alphabet, để hiển
# thị đúng thứ tự người đọc quen nhìn trên các visual khác. Chiều không có quy ước này thì giữ nguyên.
_ROMAN_VALUES = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
_NUM_PREFIX_RE = re.compile(r"^\s*(\d+(?:\.\d+)*)\s*\.\s")
_ROMAN_PREFIX_RE = re.compile(r"^\s*([IVXLCDM]+)\s*\.\s", re.IGNORECASE)


def _roman_to_int(s):
    s = s.upper()
    total, prev = 0, 0
    for ch in reversed(s):
        v = _ROMAN_VALUES.get(ch)
        if v is None:
            return None
        total += -v if v < prev else v
        prev = max(prev, v)
    return total


def _ordinal_key(value):
    s = str(value)
    m = _NUM_PREFIX_RE.match(s)
    if m:
        return tuple(int(p) for p in m.group(1).split("."))
    m = _ROMAN_PREFIX_RE.match(s)
    if m:
        n = _roman_to_int(m.group(1))
        if n is not None:
            return (n,)
    return None


_UNKNOWN_LABELS = {"không xác định", "chưa phân loại", "không phân loại"}


def order_dim_values(dim_id, values):
    # Nhãn "chưa phân loại/không xác định" luôn xếp cuối, không tính vào tỉ lệ nhận diện — nếu không,
    # một chiều chỉ có vài giá trị thật (VD F0: I/II/III) dễ bị rớt dưới ngưỡng chỉ vì có thêm 1 nhãn này.
    values = list(values)
    known = [v for v in values if str(v).strip().lower() not in _UNKNOWN_LABELS]
    unknown = [v for v in values if str(v).strip().lower() in _UNKNOWN_LABELS]
    keys = [_ordinal_key(v) for v in known]
    if not known or sum(1 for k in keys if k is not None) / len(known) < 0.8:
        return values
    ordered = [v for v, k in sorted(zip(known, keys), key=lambda vk: (0, vk[1]) if vk[1] is not None else (1, str(vk[0])))]
    return ordered + unknown


# ============================================================== nạp & làm sạch CSV

def read_csv_text(text):
    return pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)


def check_table(df, required, optional):
    cols = list(df.columns)
    missing_req = [c for c in required if c not in cols]
    missing_opt = [c for c in optional if c not in cols]
    n = len(df)
    worst_pct, worst_col = 0.0, ""
    for c in required:
        if c in cols and n:
            empty = int((df[c].astype(str).str.strip() == "").sum())
            pct = empty / n
            if pct > worst_pct:
                worst_pct, worst_col = pct, c
    return dict(missing_req=missing_req, missing_opt=missing_opt, found_opt=[c for c in optional if c in cols],
                rows=n, worst_pct=round(worst_pct, 4), worst_col=worst_col)


def to_num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", "").str.replace(" ", ""), errors="coerce")


def prep_policy_csv(df):
    d = df.copy()
    d["Ngày hiệu lực hợp đồng"] = pd.to_datetime(d["Ngày hiệu lực hợp đồng"], dayfirst=True, errors="coerce")
    d["Ngày kết thúc hiệu lực"] = pd.to_datetime(d["Ngày kết thúc hiệu lực"], dayfirst=True, errors="coerce")
    d["Phí BH phân bổ"] = to_num(d["Phí BH phân bổ"]).fillna(0.0)
    if "Loại bút toán" in d.columns:
        d["la_dieu_chinh"] = d["Loại bút toán"].astype(str).str.strip().str.upper().isin(["E0", "CAN"])
    else:
        d["la_dieu_chinh"] = False
    for dim_id, _label, csv_col, _pq in DIM_DEFS:
        d["_" + dim_id] = d[csv_col] if csv_col in d.columns else np.nan
    d = d.dropna(subset=["Ngày hiệu lực hợp đồng", "Ngày kết thúc hiệu lực"])
    d["_thang"] = d["Ngày hiệu lực hợp đồng"].dt.strftime("%Y-%m")
    return EG.add_days(d)


def prep_claims_csv(df):
    d = df.copy()
    d["Ngày xảy ra tổn thất"] = pd.to_datetime(d["Ngày xảy ra tổn thất"], dayfirst=True, errors="coerce")
    d["Incurred"] = to_num(d["Số tiền tổn thất"]).fillna(0.0)
    d["TrangThai"] = d["Trạng thái hồ sơ"] if "Trạng thái hồ sơ" in d.columns else "Đã giải quyết"
    if "Số hồ sơ" not in d.columns:
        d["Số hồ sơ"] = np.arange(len(d)).astype(str)
    for dim_id, _label, csv_col, _pq in DIM_DEFS:
        d["_" + dim_id] = d[csv_col] if csv_col in d.columns else np.nan
    d = d.dropna(subset=["Ngày xảy ra tổn thất"])
    d["_thang"] = d["Ngày xảy ra tổn thất"].dt.strftime("%Y-%m")
    return d


def merge_claims_policy_dims(pol, clm):
    """CSV Bồi thường không tự mang theo chiều của Hợp đồng (Đơn vị, Hãng xe...) — nối qua Số hợp đồng,
    đúng cách pipeline gốc làm (xem p2_prep.py). Chiều thuộc CLAIMS_NATIVE_DIMS (xưởng sửa chữa) giữ nguyên
    từ chính CSV Bồi thường vì hợp đồng không có khái niệm đó."""
    policy_dim_cols = ["_" + d[0] for d in DIM_DEFS if d[0] not in CLAIMS_NATIVE_DIMS]
    keep = ["Số hợp đồng"] + [c for c in policy_dim_cols if c in pol.columns]
    ref = pol[keep].drop_duplicates("Số hợp đồng")
    merged = clm.drop(columns=[c for c in policy_dim_cols if c in clm.columns], errors="ignore")
    return merged.merge(ref, on="Số hợp đồng", how="left")


# ============================================================== nạp dữ liệu pipeline

def _finish_dims(pol, clm):
    """Bước cuối CHUNG cho mọi nguồn Policy/Claims đã làm sạch (pipeline/PBIX, Fabric kéo toàn bộ, hay
    Live Connection kéo theo kỳ) trước khi dùng được với slice_metrics/cell_metrics: add_days() (tính
    lại tong_ngay ĐÚNG quy ước +1 của engine.py — khác cột tong_ngay p2_prep.py tự ghi ra parquet
    không +1, cột đó chỉ để tham khảo/tương thích ngược, add_days() ở đây mới là giá trị THỰC SỰ dùng),
    gắn cột "_xxx" theo DIM_DEFS, cột "_thang", ép kiểu Incurred. Tách riêng để hai đường (kéo toàn bộ
    và Live) luôn dùng chung đúng một công thức, không lệch nhau."""
    pol = EG.add_days(pol)
    clm = clm.copy()
    clm["Incurred"] = pd.to_numeric(clm["Incurred"], errors="coerce").astype("float64").fillna(0.0)
    for dim_id, _label, _csv, pq_col in DIM_DEFS:
        pol["_" + dim_id] = pol[pq_col] if pq_col in pol.columns else np.nan
        clm["_" + dim_id] = clm[pq_col] if pq_col in clm.columns else np.nan
    pol["_thang"] = pol["Ngày hiệu lực hợp đồng"].dt.strftime("%Y-%m")
    clm["_thang"] = clm["Ngày xảy ra tổn thất"].dt.strftime("%Y-%m")
    return pol, clm


def load_pipeline_data():
    polp = os.path.join(DATA_DIR, "clean_policy.parquet")
    clmp = os.path.join(DATA_DIR, "clean_claims.parquet")
    if not (os.path.isfile(polp) and os.path.isfile(clmp)):
        raise FileNotFoundError("Máy này chưa có dữ liệu cache (clean_policy.parquet / clean_claims.parquet). "
                                 "Dùng Live Fabric, hoặc 'Kéo dữ liệu từ Microsoft Fabric về' để tạo cache lần đầu.")
    return _finish_dims(pd.read_parquet(polp), pd.read_parquet(clmp))


# ============================================================== lõi tính toán (gọi engine.py)

SLICE_CACHE = WA.SessionScoped("slice")
ANALYZE_CACHE = WA.SessionScoped("analyze")


def slice_metrics(pol, clm, s, e):
    """Cache theo (id dataframe, kỳ) — đổi Hàng/Cột/Chỉ số không phải lọc lại theo ngày trên 881k dòng mỗi lần."""
    key = (id(pol), id(clm), s, e)
    if key not in SLICE_CACHE:
        pl = EG.slice_pol(pol, s, e)
        cl = EG.slice_clm(clm, s, e)
        SLICE_CACHE[key] = (EG.metrics(pl, cl), pl, cl)
    return SLICE_CACHE[key]


def cell_metrics(pl, cl, rowcol, colcol):
    """Vector hoá bằng groupby thay vì lặp Python + lọc lại cl cho từng nhóm.

    Một số chiều (vd Phân nhóm xưởng) chỉ tồn tại ở phía Bồi thường — hợp đồng chưa từng có claim thì
    không có khái niệm "xưởng sửa chữa". Với chiều như vậy không tách được Exposure/Earned theo đúng
    chiều đó; xử lý bằng cách gộp Exposure/Earned theo (các) chiều PHÍA HỢP ĐỒNG đang có, dùng chung cho
    mọi nhóm claims-only — đúng nguyên tắc "vẫn đo được tần suất trên nền exposure toàn danh mục" đã áp
    dụng trong p20_engine_data.py (DIMS_CLAIM)."""
    if rowcol not in cl.columns or colcol not in cl.columns:
        return {}
    cl2 = cl.dropna(subset=[rowcol, colcol])
    if not len(cl2):
        return {}
    g_cl = cl2.groupby([rowcol, colcol], observed=True).agg(
        claims=("Số hồ sơ", "nunique"), incurred_raw=("Incurred", "sum")).reset_index()
    os_ = (cl2.loc[cl2["TrangThai"] == "Chưa giải quyết"]
           .groupby([rowcol, colcol], observed=True)["Incurred"].sum()
           .reset_index().rename(columns={"Incurred": "os"}))
    g_cl = g_cl.merge(os_, on=[rowcol, colcol], how="left")

    row_full = rowcol in pl.columns and pl[rowcol].notna().any()
    col_full = colcol in pl.columns and pl[colcol].notna().any()

    if row_full and col_full:
        pl2 = pl.dropna(subset=[rowcol, colcol])
        g_pl = pl2.groupby([rowcol, colcol], observed=True).agg(
            exposure=("Exposure", "sum"), earned=("Earned", "sum")).reset_index()
        d = g_cl.merge(g_pl, on=[rowcol, colcol], how="outer")
    elif row_full or col_full:
        full_dim = rowcol if row_full else colcol
        pl2 = pl.dropna(subset=[full_dim])
        g_pl = pl2.groupby(full_dim, observed=True).agg(
            exposure=("Exposure", "sum"), earned=("Earned", "sum")).reset_index()
        d = g_cl.merge(g_pl, on=full_dim, how="left")
    else:
        d = g_cl.copy()
        d["exposure"] = float(pl["Exposure"].sum())
        d["earned"] = float(pl["Earned"].sum())

    d[["claims", "incurred_raw", "os", "exposure", "earned"]] = \
        d[["claims", "incurred_raw", "os", "exposure", "earned"]].fillna(0.0)
    d["incurred"] = d["incurred_raw"]  # dev=1.0 mặc định, không hiệu chỉnh dự phòng ở lát cắt ô
    d["freq"] = np.where(d["exposure"] > 0, d["claims"] / d["exposure"], 0.0)
    d["sev"] = np.where(d["claims"] > 0, d["incurred"] / d["claims"], 0.0)
    d["pp"] = np.where(d["exposure"] > 0, d["incurred"] / d["exposure"], 0.0)
    d["avgprem"] = np.where(d["exposure"] > 0, d["earned"] / d["exposure"], 0.0)
    d["lr"] = np.where(d["earned"] > 0, d["incurred"] / d["earned"], 0.0)

    cells = {}
    for _, row in d.iterrows():
        rv, cv = row[rowcol], row[colcol]
        if pd.isna(rv) or pd.isna(cv):
            continue
        cells[(str(rv), str(cv))] = dict(exposure=row["exposure"], earned=row["earned"], claims=int(row["claims"]),
                                          incurred=row["incurred"], freq=row["freq"], sev=row["sev"],
                                          pp=row["pp"], avgprem=row["avgprem"], lr=row["lr"])
    return cells


def add_ae(cells, portfolio):
    """A/E đơn giản hoá — so Incurred thật với kỳ vọng = freq*sev toàn danh mục áp lên exposure của ô.
    KHÔNG phải bản A/E chuẩn hoá 3 chiều đầy đủ đang dùng trong báo cáo chính thức (xem TRANG THAI.md mục 3)."""
    pf, ps = portfolio.get("freq", 0.0), portfolio.get("sev", 0.0)
    for m in cells.values():
        exp_inc = pf * ps * m["exposure"]
        m["ae"] = (m["incurred"] / exp_inc) if exp_inc else 0.0


def segment_filter(pl, cl, rowdim, coldim, rowval, colval):
    """Lọc đúng ô đang chọn trên heatmap. Chiều claims-only (không có ở pl) thì bỏ qua lọc phía pl cho
    chiều đó — khớp đúng ngữ nghĩa 'exposure quy về nền toàn danh mục' đã dùng trong cell_metrics."""
    seg_cl = cl
    if rowdim in cl.columns:
        seg_cl = seg_cl[seg_cl[rowdim].astype(str) == rowval]
    if coldim in cl.columns:
        seg_cl = seg_cl[seg_cl[coldim].astype(str) == colval]
    seg_pl = pl
    if rowdim in pl.columns and pl[rowdim].notna().any():
        seg_pl = seg_pl[seg_pl[rowdim].astype(str) == rowval]
    if coldim in pl.columns and pl[coldim].notna().any():
        seg_pl = seg_pl[seg_pl[coldim].astype(str) == colval]
    return seg_pl, seg_cl


def safe_bridge(m0, m1):
    """EG.bridge chia cho avgprem của m0/m1 — bọc lại để ô rỗng/không có exposure không làm sập request."""
    if not m0.get("avgprem") or not m1.get("avgprem"):
        return None
    try:
        return {k: float(v) for k, v in EG.bridge(m0, m1).items()}
    except Exception:  # noqa: BLE001
        return None


MIN_CLAIMS_FOR_DRIVER = 5
# Mẫu số dùng để tính tỷ trọng đóng góp (share) — chọn theo bản chất từng chỉ số, khớp cách engine.py
# đo lường: LR/Pure Premium/Severity/A-E quy về Incurred, Frequency quy về Exposure, Average Premium
# quy về Earned (phí thực hưởng).
METRIC_BASE = {"lr": "incurred", "pp": "incurred", "sev": "incurred", "ae": "incurred",
               "freq": "exposure", "avgprem": "earned"}


def _dim_group_stats(seg_pl, seg_cl, col, seg_metrics, metric, min_claims=MIN_CLAIMS_FOR_DRIVER):
    """Group theo một cột chiều, trả DataFrame đã tính đủ freq/sev/pp/avgprem/lr(/ae) + impact.
    impact > 0 nghĩa là nhóm đó kéo chỉ số ĐANG CHỌN cao hơn mức của cả đoạn — dấu không tự nói tốt/xấu,
    phía frontend đối chiếu với HIGHER_IS_WORSE của từng chỉ số để xếp vào tab tích cực/tiêu cực.
    `min_claims`: ngưỡng loại nhóm quá nhỏ — Khám phá nhanh (scan_drivers) dùng MIN_CLAIMS_FOR_DRIVER
    để chỉ nêu vài driver đáng tin; Đào sâu/Decomposition Tree (split_by_dim) truyền 1 để trả về ĐỦ mọi
    nhóm có ít nhất 1 hồ sơ, để người dùng tự đào và tự thấy hết — mẫu nhỏ vẫn có cờ cảnh báo riêng ở
    frontend (driverRows/dsRenderRows) chứ không bị giấu."""
    if col not in seg_cl.columns:
        return None
    cl2 = seg_cl.dropna(subset=[col])
    if not len(cl2):
        return None
    g_cl = cl2.groupby(col, observed=True).agg(
        claims=("Số hồ sơ", "nunique"), incurred=("Incurred", "sum")).reset_index()
    row_full = col in seg_pl.columns and seg_pl[col].notna().any()
    if row_full:
        pl2 = seg_pl.dropna(subset=[col])
        g_pl = pl2.groupby(col, observed=True).agg(
            exposure=("Exposure", "sum"), earned=("Earned", "sum")).reset_index()
        d = g_cl.merge(g_pl, on=col, how="outer")
    else:
        d = g_cl.copy()
        d["exposure"] = float(seg_pl["Exposure"].sum())
        d["earned"] = float(seg_pl["Earned"].sum())
    d[["claims", "incurred", "exposure", "earned"]] = d[["claims", "incurred", "exposure", "earned"]].fillna(0.0)
    d = d[d["claims"] >= min_claims]
    if not len(d):
        return None
    d["freq"] = np.where(d["exposure"] > 0, d["claims"] / d["exposure"], np.nan)
    d["sev"] = np.where(d["claims"] > 0, d["incurred"] / d["claims"], np.nan)
    d["pp"] = np.where(d["exposure"] > 0, d["incurred"] / d["exposure"], np.nan)
    d["avgprem"] = np.where(d["exposure"] > 0, d["earned"] / d["exposure"], np.nan)
    d["lr"] = np.where(d["earned"] > 0, d["incurred"] / d["earned"], np.nan)
    if metric == "ae":
        seg_pf, seg_ps = seg_metrics.get("freq", 0.0), seg_metrics.get("sev", 0.0)
        exp_inc = seg_pf * seg_ps * d["exposure"]
        d["ae"] = np.where(exp_inc > 0, d["incurred"] / exp_inc, np.nan)
    base_key = METRIC_BASE.get(metric, "incurred")
    seg_base_total = seg_metrics.get(base_key, 0.0) or 1.0
    seg_val = seg_metrics.get(metric, 0.0)
    d["share_base"] = d[base_key] / seg_base_total
    d["impact"] = d["share_base"] * (d[metric] - seg_val)
    d = d.rename(columns={col: "_cat"})
    return d


def scan_drivers(seg_pl, seg_cl, exclude_cols, seg_metrics, metric="lr", top=8):
    """Quét mọi chiều còn lại bên trong đoạn đang xem, xếp hạng nhóm con theo mức đóng góp vào chỉ số
    ĐANG CHỌN của chính đoạn đó — cùng công thức 'within effect' (share × (giá trị nhóm − giá trị đoạn))
    đã dùng trong engine.mix_within, chỉ khác là quét toàn bộ chiều còn lại thay vì một chiều cố định
    và tổng quát theo mọi chỉ số thay vì chỉ LR. Không dùng AI — thuần thống kê mô tả. Bỏ nhóm dưới
    MIN_CLAIMS_FOR_DRIVER hồ sơ để tránh cờ giả từ mẫu quá nhỏ."""
    candidates = []
    for dim_id, label, _csv, _pq in DIM_DEFS:
        col = "_" + dim_id
        if col in exclude_cols:
            continue
        d = _dim_group_stats(seg_pl, seg_cl, col, seg_metrics, metric)
        if d is None:
            continue
        for _, row in d.iterrows():
            if pd.isna(row[metric]) or pd.isna(row["impact"]):
                continue
            candidates.append(dict(
                dim=dim_id, dimLabel=label, category=str(row["_cat"]),
                metricValue=float(row[metric]), shareBase=float(row["share_base"]),
                claims=int(row["claims"]), exposure=float(row["exposure"]), impact=float(row["impact"]),
            ))
    candidates.sort(key=lambda c: -abs(c["impact"]))
    return candidates[:top]


def split_by_dim(seg_pl, seg_cl, dim_id, seg_metrics, metric, top=100):
    """Bẻ đoạn hiện tại theo ĐÚNG MỘT chiều người dùng chọn — dùng cho Decomposition Tree/Đào sâu, khác
    scan_drivers ở chỗ không tự quét chọn chiều tốt nhất mà theo đúng chiều được yêu cầu. min_claims=1
    (khác Khám phá nhanh): người dùng đã chủ động chọn đúng chiều này để đào, nên trả về ĐỦ mọi nhóm dù
    nhỏ, không giấu — mẫu nhỏ gắn cờ cảnh báo riêng ở frontend chứ không loại khỏi kết quả."""
    label = next((lb for did, lb, _c, _p in DIM_DEFS if did == dim_id), dim_id)
    col = "_" + dim_id
    d = _dim_group_stats(seg_pl, seg_cl, col, seg_metrics, metric, min_claims=1)
    if d is None:
        return []
    out = []
    for _, row in d.iterrows():
        if pd.isna(row[metric]) or pd.isna(row["impact"]):
            continue
        out.append(dict(
            dim=dim_id, dimLabel=label, category=str(row["_cat"]),
            metricValue=float(row[metric]), shareBase=float(row["share_base"]),
            claims=int(row["claims"]), exposure=float(row["exposure"]), impact=float(row["impact"]),
        ))
    out.sort(key=lambda c: -abs(c["impact"]))
    return out[:top]


def top_categories(cells, n=15):
    """n=None/0 -> không cắt, trả hết. Xếp theo DOANH THU (Earned = phí thực hưởng), không phải exposure —
    đúng yêu cầu 'top hiển thị sort theo doanh thu'. Luôn kèm coverage để không giấu phần bị cắt bớt."""
    rev_by_row, rev_by_col = {}, {}
    for (r, c), m in cells.items():
        rev_by_row[r] = rev_by_row.get(r, 0) + m["earned"]
        rev_by_col[c] = rev_by_col.get(c, 0) + m["earned"]
    all_rows = sorted(rev_by_row, key=lambda k: -rev_by_row[k])
    all_cols = sorted(rev_by_col, key=lambda k: -rev_by_col[k])
    rows = all_rows if not n else all_rows[:n]
    cols = all_cols if not n else all_cols[:n]
    tot_row = sum(rev_by_row.values()) or 1.0
    tot_col = sum(rev_by_col.values()) or 1.0
    coverage = dict(
        rowsShown=len(rows), rowsTotal=len(all_rows), rowsCoverage=sum(rev_by_row[r] for r in rows) / tot_row,
        colsShown=len(cols), colsTotal=len(all_cols), colsCoverage=sum(rev_by_col[c] for c in cols) / tot_col,
    )
    return rows, cols, coverage


def shift_period(s, e, mode):
    s, e = pd.Timestamp(s), pd.Timestamp(e)
    if mode == "yoy":
        return s - pd.DateOffset(years=1), e - pd.DateOffset(years=1)
    if mode == "mom":
        span = (e - s).days + 1
        return s - pd.Timedelta(days=span), s - pd.Timedelta(days=1)
    return None, None


# ============================================================== handlers API

def h_validate(body):
    source = body.get("source")
    pol_check = clm_check = None
    pol_ok = clm_ok = False
    date_ranges = {}
    SLICE_CACHE.clear()
    ANALYZE_CACHE.clear()

    if source == "csv":
        if body.get("policyCsv"):
            raw = read_csv_text(body["policyCsv"])
            pol_check = check_table(raw, POLICY_REQUIRED, POLICY_OPTIONAL)
            if not pol_check["missing_req"]:
                pol = prep_policy_csv(raw)
                STORE["pol"] = pol
                pol_ok = True
                date_ranges["policy"] = [str(pol["Ngày hiệu lực hợp đồng"].min().date()),
                                          str(pol["Ngày kết thúc hiệu lực"].max().date())]
        if body.get("claimsCsv"):
            raw = read_csv_text(body["claimsCsv"])
            clm_check = check_table(raw, CLAIMS_REQUIRED, CLAIMS_OPTIONAL)
            if not clm_check["missing_req"]:
                clm = prep_claims_csv(raw)
                STORE["clm"] = clm
                clm_ok = True
                date_ranges["claims"] = [str(clm["Ngày xảy ra tổn thất"].min().date()),
                                          str(clm["Ngày xảy ra tổn thất"].max().date())]
        if pol_ok and clm_ok:
            STORE["clm"] = merge_claims_policy_dims(STORE["pol"], STORE["clm"])
        STORE["source"] = "csv"
    elif source == "pipeline":
        pol, clm = load_pipeline_data()
        STORE["pol"], STORE["clm"], STORE["source"] = pol, clm, "pipeline"
        pol_ok = clm_ok = True
        date_ranges["policy"] = [str(pol["Ngày hiệu lực hợp đồng"].min().date()),
                                  str(pol["Ngày kết thúc hiệu lực"].max().date())]
        date_ranges["claims"] = [str(clm["Ngày xảy ra tổn thất"].min().date()),
                                  str(clm["Ngày xảy ra tổn thất"].max().date())]
        pol_check = dict(missing_req=[], missing_opt=[], found_opt=[d[2] for d in DIM_DEFS],
                          rows=len(pol), worst_pct=0.0, worst_col="")
        clm_check = dict(missing_req=[], missing_opt=[], found_opt=[d[2] for d in DIM_DEFS],
                          rows=len(clm), worst_pct=0.0, worst_col="")
    else:
        return {"error": "Nguồn dữ liệu không hợp lệ."}

    dims_available = []
    if pol_ok and STORE["pol"] is not None:
        pol_df, clm_df = STORE["pol"], STORE["clm"]
        for d in DIM_DEFS:
            col = "_" + d[0]
            has_pol = col in pol_df.columns and pol_df[col].notna().any()
            has_clm = clm_ok and clm_df is not None and col in clm_df.columns and clm_df[col].notna().any()
            if has_pol or has_clm:
                dims_available.append(d[0])
        dims_available.append("thang")

    readiness = {
        "lr": pol_ok and clm_ok, "freq": pol_ok and clm_ok, "sev": clm_ok,
        "pp": pol_ok and clm_ok, "avgprem": pol_ok, "ae": pol_ok and clm_ok,
    }
    STORE["loadedAt"] = time.strftime("%Y-%m-%d %H:%M:%S")
    result = {
        "policyCheck": pol_check, "claimsCheck": clm_check,
        "policyOk": pol_ok, "claimsOk": clm_ok,
        "dateRanges": date_ranges, "readiness": readiness, "dimsAvailable": dims_available,
    }
    STORE["lastValidate"] = result  # để link Share (guest) đọc lại mà không cần quyền validate
    return result


def _apply_focus(pl, cl, body):
    """Chế độ Đào sâu chi tiết — lọc pl/cl theo MỘT CHUỖI chiều+giá trị (nhiều nấc, người dùng tự thêm/
    xoá ở UI) TRƯỚC khi dựng heatmap theo hai chiều còn lại. Mỗi nấc lọc thêm một lớp, thứ tự không quan
    trọng vì các điều kiện độc lập (AND). Chiều claims-native (vd Gara) thì bỏ qua lọc phía pl cho nấc
    đó, giống segment_filter(). `focusPath`: [{"dim","val"}, ...]; vẫn nhận focusDim/focusVal đơn lẻ
    (dạng cũ) để không phá tương thích nếu có nơi khác còn gọi kiểu cũ."""
    path = body.get("focusPath") or ([]
        if not (body.get("focusDim") and body.get("focusVal"))
        else [{"dim": body["focusDim"], "val": body["focusVal"]}])
    applied = False
    for step in path:
        fdim, fval = step.get("dim"), step.get("val")
        if not fdim or not fval:
            continue
        col = "_" + fdim
        if col in cl.columns:
            cl = cl[cl[col].astype(str) == fval]
        if col in pl.columns and pl[col].notna().any():
            pl = pl[pl[col].astype(str) == fval]
        applied = True
    return pl, cl, applied


def h_analyze(body):
    if STORE["pol"] is None or STORE["clm"] is None:
        return {"error": "Chưa có dữ liệu đã nạp. Hãy hoàn tất Bước 1."}
    top_n = body.get("topN", 15) or None
    focus_key = tuple(sorted((s.get("dim"), s.get("val")) for s in (body.get("focusPath") or [])))
    cache_key = (body.get("rowDim"), body.get("colDim"), body.get("metric"),
                 body.get("start"), body.get("end"), body.get("mode", "current"), top_n, focus_key)
    if cache_key in ANALYZE_CACHE:
        return ANALYZE_CACHE[cache_key]

    pol, clm = STORE["pol"], STORE["clm"]
    rowdim = "_" + body["rowDim"]
    coldim = "_" + body["colDim"]
    metric = body["metric"]
    s, e = body["start"], body["end"]
    mode = body.get("mode", "current")

    _full_portfolio, pl, cl = slice_metrics(pol, clm, s, e)
    pl, cl, focused = _apply_focus(pl, cl, body)
    portfolio = EG.metrics(pl, cl) if focused else _full_portfolio
    cells = cell_metrics(pl, cl, rowdim, coldim)
    if metric == "ae":
        add_ae(cells, portfolio)
    rows, cols, coverage = top_categories(cells, top_n)
    rows = order_dim_values(body["rowDim"], rows)
    cols = order_dim_values(body["colDim"], cols)

    result = {}
    if mode == "current":
        for (r, c), m in cells.items():
            if r in rows and c in cols:
                result[f"{r}|{c}"] = {"value": m.get(METRIC_KEYS[metric]), "exposure": m["exposure"], "claims": m["claims"]}
    else:
        ps, pe = shift_period(s, e, mode)
        _full_prev, ppl, pcl = slice_metrics(pol, clm, ps.date().isoformat(), pe.date().isoformat())
        ppl, pcl, _ = _apply_focus(ppl, pcl, body)
        prev_portfolio = EG.metrics(ppl, pcl) if focused else _full_prev
        prev_cells = cell_metrics(ppl, pcl, rowdim, coldim)
        if metric == "ae":
            add_ae(prev_cells, prev_portfolio)
        for (r, c), m in cells.items():
            if r not in rows or c not in cols:
                continue
            pm = prev_cells.get((r, c))
            cur_v = m.get(METRIC_KEYS[metric])
            prev_v = pm.get(METRIC_KEYS[metric]) if pm else None
            delta = (cur_v - prev_v) if (prev_v is not None and cur_v is not None) else None
            result[f"{r}|{c}"] = {"value": cur_v, "prev": prev_v, "delta": delta,
                                   "exposure": m["exposure"], "claims": m["claims"]}

    out = {
        "rows": rows, "cols": cols, "cells": result, "mode": mode, "coverage": coverage,
        "portfolio": {"value": portfolio.get(METRIC_KEYS[metric]), "exposure": portfolio["exposure"],
                      "earned": portfolio["earned"], "claims": portfolio["claims"]},
    }
    ANALYZE_CACHE[cache_key] = out
    return out


# Live Connection: gọi thẳng DAX measure có sẵn trong PBIX (đúng theo quy định của dự án — công thức
# các chỉ số bắt theo logic file Power BI) thay vì kéo dữ liệu thô về tính lại bằng engine.py. Đã thử
# đường kéo-thô-theo-kỳ trước đó và bỏ: "DT kế toán" là bảng CHI TIẾT KẾ TOÁN, lọc theo cửa sổ ngày gần
# như không giảm được số dòng khớp nên không nhanh hơn kéo hết.
#
# CHUẨN ĐỐI CHIẾU của đường Live là BÁO CÁO POWER BI, không phải pipeline. Người dùng mở báo cáo BI để
# đối chiếu, nên Live phải gọi đúng measure mà báo cáo dùng, dưới đúng bộ lọc mà báo cáo áp. Đã đo:
# LR/Frequency/Severity/Phí BQ do app tự tính từ Exposure/Earned/Incurred/Claims khớp measure của báo
# cáo tới 0,000000 trên mọi nhóm của mọi chiều dưới đây. Hệ quả đã biết và chấp nhận: Live lệch khoảng
# 0,4-0,8% so với nguồn kéo-toàn-bộ trong chính app, vì pipeline cộng thêm 1 ngày khi đếm (xem
# engine.py add_days) còn measure của báo cáo thì không. Muốn hai nguồn thống nhất phải sửa ở pipeline.
LIVE_DIM_MAP = {
    "donvi": ("Mã đơn vị", "ĐƠN VỊ"),
    "hangxe": ("Phân loại hiệu xe hãng xe", "Hãng xe làm sạch"),
    "kenh": ("Kênh khai thác", "Kênh"),
    "daigiatri": ("Giá trị xe", "Giá trị xe"),
    "mucdich": ("Phân nhóm loại xe", "MDSD"),
    "nhomrr": ("Địa bàn hoạt động", "Phân nhóm rủi ro từng địa bàn"),
    "thoigiansd": ("DT kế toán", "Nhóm Thời gian sử dụng xe"),
    "hieuxe": ("Phân loại hiệu xe hãng xe", "Hiệu xe"),
    "dongxe": ("Phân loại hiệu xe hãng xe", "Dòng xe"),
    "phankhuc": ("Phân loại hiệu xe hãng xe", "Phân khúc"),
    "nhienlieu": ("Phân loại hiệu xe hãng xe", "Nhiên liệu"),
    "sochongoi": ("Phân loại hiệu xe hãng xe", "Số chỗ ngồi"),
    "kieuthanxe": ("Phân loại hiệu xe hãng xe", "Kiểu thân xe"),
    "phongkd": ("Mã phòng", "Tên phòng"),
    "canbokt": ("Mã cán bộ", "Tên cán bộ khai thác"),
    "doitac": ("Mã nguồn DV", "Nhóm nguồn khai thác"),
    "diemban": ("Mã nguồn DV", "Tên tắt"),
    "sochoden": ("Phân nhóm loại xe", "Số chỗ (đến)"),
    "trongtaikg": ("Phân nhóm loại xe", "Trọng tải kg (đến)"),
    "diaban": ("Địa bàn hoạt động", "Địa bàn (theo mã biển số)"),
    "nhomgara": ("Gara sửa chữa", "Phân nhóm SH/GR"),
    "garatt": ("Gara sửa chữa", "TENTHUONGGOI_TEN_TAT"),
    # Chiều chỉ có bên Bồi thường: phía Hợp đồng không cắt theo nó được nên mỗi nhóm nhận nguyên
    # Exposure của cả đoạn, cộng các nhóm lại sẽ vượt tổng danh mục. Báo cáo BI hành xử y hệt, và
    # đường kéo-toàn-bộ của app cũng vậy (xem CLAIMS_NATIVE_DIMS).
    "nhommucdo": ("Bồi thường CGQ", "Phân loại số tiền tổn thất"),
    "dongco": None,  # bảng "Động cơ" rỗng trên nguồn Fabric, truy vấn báo lỗi
    "loaixe_f0": ("Loại xe", "F0"),
    "loaixe_f1": ("Loại xe", "F1"),
    "loaixe_f2": ("Loại xe", "F2"),
    "ftnds": ("Loại xe", "FTNDS"),
}

def _live_dim_table_col(dim_id):
    return LIVE_DIM_MAP.get(dim_id)


def _live_dim_supported(dim_id):
    return bool(LIVE_DIM_MAP.get(dim_id))


# Lý do riêng cho từng chiều chưa dùng được, để người dùng biết đây là hạn chế của nguồn dữ liệu chứ
# không phải lỗi thao tác. Chiều nào không có dòng riêng thì dùng câu chung.
LIVE_DIM_UNSUPPORTED_REASON = {
    "dongco": "bảng \"Động cơ\" trên Fabric đang rỗng nên không cắt theo chiều này được",
}


def _live_dim_unsupported_msg(dim_id):
    label = next((lb for did, lb, _c, _p in DIM_DEFS if did == dim_id), dim_id)
    ly_do = LIVE_DIM_UNSUPPORTED_REASON.get(dim_id, "chiều này chưa có trong model Fabric của báo cáo")
    return (f"Chiều \"{label}\" chưa dùng được ở chế độ kết nối trực tiếp: {ly_do}. "
            f"Hãy chọn chiều khác, hoặc dùng nguồn kéo dữ liệu về để xem chiều này.")


LIVE_GRID_CACHE = WA.SessionScoped("live_grid")
LIVE_ANALYZE_CACHE = WA.SessionScoped("live_analyze")
LIVE_CACHE_LOCK = threading.Lock()


def _dax_date_filter(s, e):
    sy, sm, sd = (int(x) for x in s.split("-"))
    ey, em, ed = (int(x) for x in e.split("-"))
    return (f"'Năm tài chính'[Date] >= DATE({sy},{sm},{sd}) "
            f"&& 'Năm tài chính'[Date] <= DATE({ey},{em},{ed})")


# Theo phiên: RLS theo role "Nghiệp vụ" có thể trả bộ mã VCX khác nhau cho từng người.
_LIVE_VCX_CACHE = WA.SessionScoped("live_vcx")
_LIVE_VCX_LOCK = threading.Lock()


# Sự kiện thiên tai bị loại ở FILTER CẤP BÁO CÁO của "Báo cáo quản trị BH XCG" (đọc trực tiếp từ
# Report/definition/report.json trong file .pbix). Áp cùng danh sách này để Live khớp báo cáo khi xem
# các kỳ có hai trận lụt đó. Kỳ 2026-01→07 không chứa chúng nên số không đổi.
LIVE_STORM_EXCLUDED = ["Lụt Nam Trung Bộ", "Lụt Huế - Đà Nẵng"]


def _live_vcx_filters():
    """Bộ lọc CỐ ĐỊNH bắt buộc cho MỌI truy vấn Live, dựng lại đúng filter context của BÁO CÁO Power BI:
    nghiệp vụ "5.3. BH VCX ô tô" + 'Nguồn dữ liệu'="DBV" (filter cấp báo cáo) + loại sự kiện thiên tai
    (filter cấp báo cáo). LÝ DO PHẢI CÓ: measure DAX trong PBIX (Đếm số hợp đồng/Phí thực hưởng/CPBT gốc
    năm nay) KHÔNG tự lọc trong công thức — trên Power BI các điều kiện này nằm ở filter cấp báo cáo/trang,
    một filter context CHỈ tồn tại trong report, không đi kèm khi gọi measure thẳng qua executeQueries.
    Thiếu bước lọc nghiệp vụ thì Exposure lệch tới ~3,7 lần (650.914 so với 174.162 đúng, kỳ 2026-01→07).
    Đã kiểm chứng: với bộ lọc này, mọi chỉ số khớp measure của báo cáo tới 0,000000."""
    with _LIVE_VCX_LOCK:
        if _LIVE_VCX_CACHE.get("filters") is not None:
            return _LIVE_VCX_CACHE["filters"]
        _nv, vcx = FABRIC._vcx_codes()
        if not vcx:
            raise RuntimeError("Không tìm thấy mã nghiệp vụ VCX ô tô trên Fabric.")
        vcx_set = "{" + ", ".join('"' + str(v).replace('"', '""') + '"' for v in vcx) + "}"
        storm_set = "{" + ", ".join('"' + s.replace('"', '""') + '"' for s in LIVE_STORM_EXCLUDED) + "}"
        filters = [
            f"'DT kế toán'[Mã NV] IN {vcx_set}",
            '\'DT kế toán\'[Nguồn dữ liệu] = "DBV"',
            f"'Bồi thường CGQ'[Mã NV] IN {vcx_set}",
            f"'Bồi thường DGQ'[Mã NV] IN {vcx_set}",
            f"NOT('Sự kiện bão'[Sự kiện bão] IN {storm_set})",
        ]
        _LIVE_VCX_CACHE["filters"] = filters
        return filters




def _live_base_filters(s, e, path=None):
    """Danh sách điều kiện DAX dùng chung cho MỌI truy vấn Live — kỳ + phạm vi nghiệp vụ VCX ô tô/DBV,
    cộng thêm path (Decomposition Tree) nếu có. MỌI hàm _live_* dựng DAX phải đi qua đây, không tự ghép
    tay _dax_date_filter() một mình — thiếu bước lọc nghiệp vụ là lỗi đã xảy ra thật (xem _live_vcx_filters)."""
    filters = [_dax_date_filter(s, e)] + _live_vcx_filters()
    if path:
        filters += _dax_path_filters(path)
    return filters


LIVE_RAW_MEASURES = (
    '"Exposure", [Đếm số hợp đồng], "Earned", [Phí thực hưởng], "Incurred", [CPBT gốc năm nay], '
    '"Claims", DISTINCTCOUNT(\'Bồi thường CGQ\'[Số hồ sơ]) + DISTINCTCOUNT(\'Bồi thường DGQ\'[Số hồ sơ])'
)


def _row_to_metrics(r):
    """Tự tính freq/sev/pp/avgprem/lr từ 4 giá trị thô (Exposure/Earned/Incurred/Claims) thay vì đọc
    measure LR/Freq/Sev có sẵn — để công thức suy ra giống HỆT cell_metrics() ở đường kéo-toàn-bộ, tránh
    lệch do cách measure tự xử lý mẫu số 0 khác cell_metrics()."""
    exposure = float(r.get("[Exposure]") or 0.0)
    earned = float(r.get("[Earned]") or 0.0)
    incurred = float(r.get("[Incurred]") or 0.0)
    claims = int(r.get("[Claims]") or 0)
    return dict(
        exposure=exposure, earned=earned, incurred=incurred, claims=claims,
        freq=(claims / exposure) if exposure else 0.0,
        sev=(incurred / claims) if claims else 0.0,
        pp=(incurred / exposure) if exposure else 0.0,
        avgprem=(earned / exposure) if exposure else 0.0,
        lr=(incurred / earned) if earned else 0.0,
    )


def _live_cell_metrics(rowdim_id, coldim_id, s, e):
    rt, rc = _live_dim_table_col(rowdim_id)
    ct, cc = _live_dim_table_col(coldim_id)
    filt_expr = ",\n    ".join(_live_base_filters(s, e))
    dax = (
        "EVALUATE\nCALCULATETABLE(\n    SUMMARIZECOLUMNS(\n"
        f"        '{rt}'[{rc}], '{ct}'[{cc}],\n        {LIVE_RAW_MEASURES}\n    ),\n"
        f"    {filt_expr}\n)"
    )
    raw = FABRIC.run_dax(dax)
    rkey, ckey = f"{rt}[{rc}]", f"{ct}[{cc}]"
    cells = {}
    for r in raw:
        rv, cv = r.get(rkey), r.get(ckey)
        if rv is None or cv is None:
            continue
        cells[(str(rv), str(cv))] = _row_to_metrics(r)
    return cells


def _live_portfolio_metrics(s, e):
    filt_expr = ",\n    ".join(_live_base_filters(s, e))
    dax = (
        "EVALUATE\nCALCULATETABLE(\n"
        f"    ROW({LIVE_RAW_MEASURES}),\n    {filt_expr}\n)"
    )
    raw = FABRIC.run_dax(dax)
    return _row_to_metrics(raw[0] if raw else {})


def _live_grid(rowdim_id, coldim_id, s, e):
    """cells cho đúng lưới (rowDim,colDim,kỳ) — cache trong phiên để đổi Chỉ số/TopN trên CÙNG lưới
    không gọi lại Fabric. ms=0 nghĩa là lấy từ bộ nhớ, không phải 0 giây thật."""
    key = ("grid", rowdim_id, coldim_id, s, e)
    with LIVE_CACHE_LOCK:
        cached = LIVE_GRID_CACHE.get(key)
    if cached is not None:
        return cached, 0.0
    t0 = time.time()
    cells = _live_cell_metrics(rowdim_id, coldim_id, s, e)
    ms = (time.time() - t0) * 1000
    with LIVE_CACHE_LOCK:
        LIVE_GRID_CACHE[key] = cells
    return cells, ms


def _live_portfolio(s, e):
    key = ("portfolio", s, e)
    with LIVE_CACHE_LOCK:
        cached = LIVE_GRID_CACHE.get(key)
    if cached is not None:
        return cached, 0.0
    t0 = time.time()
    m = _live_portfolio_metrics(s, e)
    ms = (time.time() - t0) * 1000
    with LIVE_CACHE_LOCK:
        LIVE_GRID_CACHE[key] = m
    return m, ms


def h_fabric_live_analyze(body):
    """Live Connection: đọc thẳng measure DAX có sẵn trong PBIX (Đếm số hợp đồng/Phí thực hưởng/CPBT
    gốc năm nay), nhóm theo LIVE_DIM_MAP qua SUMMARIZECOLUMNS — không kéo dữ liệu thô, không tự viết
    lại công thức Exposure/Earned. Chỉ phục vụ heatmap Tổng quát/Đào sâu chi tiết — không hỗ trợ Khám
    phá nhanh/Đào sâu/Decomposition Tree ở chế độ này. Trả kèm pullMs/totalMs để so sánh hiệu năng với
    chế độ kéo-toàn-bộ."""
    if not FABRIC:
        return {"error": "Máy này chưa nạp được kết nối Fabric."}
    if not fabric_token():
        return {"error": "Chưa đăng nhập Fabric hoặc phiên đã hết hạn — đăng nhập lại trước."}

    row_dim_id, col_dim_id = body["rowDim"], body["colDim"]
    if not _live_dim_supported(row_dim_id):
        return {"error": _live_dim_unsupported_msg(row_dim_id)}
    if not _live_dim_supported(col_dim_id):
        return {"error": _live_dim_unsupported_msg(col_dim_id)}

    t0 = time.time()
    top_n = body.get("topN", 15) or None
    metric = body["metric"]
    s, e = body["start"], body["end"]
    mode = body.get("mode", "current")
    cache_key = (row_dim_id, col_dim_id, metric, s, e, mode, top_n)
    if cache_key in LIVE_ANALYZE_CACHE:
        out = dict(LIVE_ANALYZE_CACHE[cache_key])
        out["pullMs"] = 0.0
        out["totalMs"] = (time.time() - t0) * 1000
        out["fromCache"] = True
        return out

    try:
        cells, ms1 = _live_grid(row_dim_id, col_dim_id, s, e)
        portfolio, ms2 = _live_portfolio(s, e)
    except Exception as ex:  # noqa: BLE001
        return {"error": f"Truy vấn Live từ Fabric lỗi: {ex}"}
    pull_ms = ms1 + ms2

    if metric == "ae":
        add_ae(cells, portfolio)
    rows, cols, coverage = top_categories(cells, top_n)
    rows = order_dim_values(row_dim_id, rows)
    cols = order_dim_values(col_dim_id, cols)

    result = {}
    if mode == "current":
        for (r, c), m in cells.items():
            if r in rows and c in cols:
                result[f"{r}|{c}"] = {"value": m.get(METRIC_KEYS[metric]), "exposure": m["exposure"], "claims": m["claims"]}
    else:
        ps, pe = shift_period(s, e, mode)
        ps_s, pe_s = ps.date().isoformat(), pe.date().isoformat()
        try:
            prev_cells, ms3 = _live_grid(row_dim_id, col_dim_id, ps_s, pe_s)
            prev_portfolio, ms4 = _live_portfolio(ps_s, pe_s)
        except Exception as ex:  # noqa: BLE001
            return {"error": f"Truy vấn Live kỳ so sánh từ Fabric lỗi: {ex}"}
        pull_ms += ms3 + ms4
        if metric == "ae":
            add_ae(prev_cells, prev_portfolio)
        for (r, c), m in cells.items():
            if r not in rows or c not in cols:
                continue
            pm = prev_cells.get((r, c))
            cur_v = m.get(METRIC_KEYS[metric])
            prev_v = pm.get(METRIC_KEYS[metric]) if pm else None
            delta = (cur_v - prev_v) if (prev_v is not None and cur_v is not None) else None
            result[f"{r}|{c}"] = {"value": cur_v, "prev": prev_v, "delta": delta,
                                   "exposure": m["exposure"], "claims": m["claims"]}

    out = {
        "rows": rows, "cols": cols, "cells": result, "mode": mode, "coverage": coverage,
        "portfolio": {"value": portfolio.get(METRIC_KEYS[metric]), "exposure": portfolio["exposure"],
                      "earned": portfolio["earned"], "claims": portfolio["claims"]},
    }
    LIVE_ANALYZE_CACHE[cache_key] = dict(out)
    out["pullMs"] = pull_ms
    out["totalMs"] = (time.time() - t0) * 1000
    out["fromCache"] = False
    return out


def _dax_path_filters(path):
    """path: [{'dim','val'}] — trả list điều kiện DAX 'Bảng'[Cột] = "giá trị"."""
    filters = []
    for step in path:
        t, c = _live_dim_table_col(step["dim"])
        v = str(step["val"]).replace('"', '""')
        filters.append(f'\'{t}\'[{c}] = "{v}"')
    return filters


def _live_segment_metrics(path, s, e):
    filt_expr = ",\n    ".join(_live_base_filters(s, e, path))
    dax = f"EVALUATE\nCALCULATETABLE(\n    ROW({LIVE_RAW_MEASURES}),\n    {filt_expr}\n)"
    raw = FABRIC.run_dax(dax)
    return _row_to_metrics(raw[0] if raw else {})


def _live_split_candidates(path, split_dim_id, s, e):
    t, c = _live_dim_table_col(split_dim_id)
    filt_expr = ",\n    ".join(_live_base_filters(s, e, path))
    dax = (f"EVALUATE\nCALCULATETABLE(\n    SUMMARIZECOLUMNS(\n        '{t}'[{c}],\n        {LIVE_RAW_MEASURES}\n    ),\n"
           f"    {filt_expr}\n)")
    raw = FABRIC.run_dax(dax)
    key = f"{t}[{c}]"
    out = []
    for r in raw:
        v = r.get(key)
        if v is None:
            continue
        out.append((str(v), _row_to_metrics(r)))
    return out


def _live_split(path, dim_id, s, e):
    """Bọc cache cho _live_split_candidates — Khám phá nhanh gọi hàm này cho từng chiều còn lại, đổi
    Chỉ số trên CÙNG đoạn không truy vấn lại Fabric."""
    key = ("split", dim_id, tuple((p["dim"], p["val"]) for p in path), s, e)
    with LIVE_CACHE_LOCK:
        cached = LIVE_GRID_CACHE.get(key)
    if cached is not None:
        return cached, 0.0
    t0 = time.time()
    raw = _live_split_candidates(path, dim_id, s, e)
    ms = (time.time() - t0) * 1000
    with LIVE_CACHE_LOCK:
        LIVE_GRID_CACHE[key] = raw
    return raw, ms


def _live_candidates_from_raw(dim_id, raw_candidates, seg_metrics, metric, min_claims=1):
    """Cùng công thức impact = share_base × (giá trị nhóm − giá trị đoạn) như _dim_group_stats() ở
    đường kéo-toàn-bộ — chỉ khác nguồn dữ liệu đầu vào (raw_candidates: [(category, metrics_dict), ...]
    từ _live_split thay vì groupby pandas)."""
    label = next((lb for did, lb, _c, _p in DIM_DEFS if did == dim_id), dim_id)
    base_key = METRIC_BASE.get(metric, "incurred")
    seg_base_total = seg_metrics.get(base_key, 0.0) or 1.0
    seg_val = seg_metrics.get(metric, 0.0)
    seg_pf, seg_ps = seg_metrics.get("freq", 0.0), seg_metrics.get("sev", 0.0)
    out = []
    for cat, m in raw_candidates:
        if m["claims"] < min_claims:
            continue
        if metric == "ae":
            exp_inc = seg_pf * seg_ps * m["exposure"]
            m["ae"] = (m["incurred"] / exp_inc) if exp_inc else 0.0
        mv = m.get(metric)
        if mv is None:
            continue
        share_base = m[base_key] / seg_base_total
        impact = share_base * (mv - seg_val)
        out.append(dict(dim=dim_id, dimLabel=label, category=cat, metricValue=float(mv),
                         shareBase=float(share_base), claims=int(m["claims"]),
                         exposure=float(m["exposure"]), impact=float(impact)))
    return out


def _live_scan_drivers(path, seg_metrics, metric, s, e, top=24):
    """Tương đương scan_drivers() ở đường kéo-toàn-bộ nhưng quét bằng DAX — mỗi chiều còn lại trong
    LIVE_DIM_MAP là MỘT lượt gọi Fabric riêng (không gộp được vì SUMMARIZECOLUMNS theo nhiều chiều độc
    lập cùng lúc sẽ ra tích Descartes chứ không phải quét từng chiều), nên có thể mất 1-3 phút cho một
    lần Khám phá nhanh — chậm hơn hẳn bản kéo-toàn-bộ (quét cục bộ bằng pandas, gần như tức thời)."""
    used_dims = {step["dim"] for step in path}
    candidates = []
    for dim_id in LIVE_DIM_MAP:
        if not _live_dim_supported(dim_id) or dim_id in used_dims:
            continue
        raw, _ = _live_split(path, dim_id, s, e)
        candidates.extend(_live_candidates_from_raw(dim_id, raw, seg_metrics, metric, min_claims=MIN_CLAIMS_FOR_DRIVER))
    candidates.sort(key=lambda c: -abs(c["impact"]))
    return candidates[:top]


LIVE_CLAIMS_EXPORT_ROW_CAP = 20000


def _live_claims_union_expr():
    """CGQ/DGQ không có cấu trúc "chi tiết kế toán" như DT kế toán (mỗi dòng là một hồ sơ thật) nên an
    toàn để đọc thẳng — chỉ tính Incurred/TrangThai lại y hệt công thức p2_prep.clean() (cộng dồn 2 cột
    tiền, gán trạng thái cố định theo bảng nguồn) chứ không suy luận gì thêm."""
    return (
        "UNION(\n"
        "    SELECTCOLUMNS('Bồi thường CGQ',\n"
        '        "SoHoSo", \'Bồi thường CGQ\'[Số hồ sơ],\n'
        '        "SoHopDong", \'Bồi thường CGQ\'[Số hợp đồng],\n'
        '        "NgayTonThat", \'Bồi thường CGQ\'[Ngày xảy ra tổn thất],\n'
        '        "Incurred", \'Bồi thường CGQ\'[Số tiền tổn thất phân bổ] + \'Bồi thường CGQ\'[Phí giám định phân bổ],\n'
        '        "TrangThai", "Chưa giải quyết"\n'
        "    ),\n"
        "    SELECTCOLUMNS('Bồi thường DGQ',\n"
        '        "SoHoSo", \'Bồi thường DGQ\'[Số hồ sơ],\n'
        '        "SoHopDong", \'Bồi thường DGQ\'[Số hợp đồng],\n'
        '        "NgayTonThat", \'Bồi thường DGQ\'[Ngày xảy ra tổn thất],\n'
        '        "Incurred", \'Bồi thường DGQ\'[Số tiền bồi thường phân bổ] + \'Bồi thường DGQ\'[Phí giám định phân bổ],\n'
        '        "TrangThai", "Đã giải quyết"\n'
        "    )\n"
        ")"
    )


def _live_claims_count(path, s, e):
    filt_expr = ",\n    ".join(_live_base_filters(s, e, path))
    dax = (f"EVALUATE\nROW(\n    \"N\", COUNTROWS(CALCULATETABLE(\n        {_live_claims_union_expr()},\n"
           f"        {filt_expr}\n    ))\n)")
    raw = FABRIC.run_dax(dax)
    return int((raw[0] if raw else {}).get("[N]") or 0)


def _live_claims_rows(path, s, e, limit=None):
    """limit=None kéo hết (đã kiểm tra LIVE_CLAIMS_EXPORT_ROW_CAP trước ở nơi gọi) — dùng cho xuất Excel;
    limit=50 chỉ lấy top Incurred qua TOPN — dùng cho xem trước."""
    filt_expr = ",\n    ".join(_live_base_filters(s, e, path))
    union_expr = _live_claims_union_expr()
    if limit:
        dax = (f"EVALUATE\nTOPN({limit},\n    CALCULATETABLE(\n        {union_expr},\n        {filt_expr}\n    ),\n"
               f"    [Incurred], DESC\n)")
    else:
        dax = f"EVALUATE\nCALCULATETABLE(\n    {union_expr},\n    {filt_expr}\n)"
    raw = FABRIC.run_dax(dax)
    rows = [dict(soHoSo=str(r.get("[SoHoSo]") or ""), soHopDong=str(r.get("[SoHopDong]") or ""),
                 ngayTonThat=str(r.get("[NgayTonThat]") or "")[:10],
                 incurred=float(r.get("[Incurred]") or 0.0), trangThai=str(r.get("[TrangThai]") or ""))
            for r in raw]
    if not limit:
        rows.sort(key=lambda r: -r["incurred"])
    return rows


def _live_path_error(path):
    for step in path:
        if not _live_dim_supported(step.get("dim")):
            return _live_dim_unsupported_msg(step.get("dim"))
    return None


def h_fabric_live_drilldown(body):
    """Đào sâu chi tiết đầy đủ cho Live Connection — KPI + bridge F→S→AP so với toàn danh mục + so kỳ
    trước (yoy/mom) + Khám phá nhanh, cùng hình dạng trả về như h_drilldown để dùng lại nguyên UI. Khám
    phá nhanh chậm hơn hẳn bản kéo-toàn-bộ vì phải quét từng chiều bằng một lượt gọi Fabric riêng — xem
    _live_scan_drivers()."""
    if not FABRIC:
        return {"error": "Máy này chưa nạp được kết nối Fabric."}
    if not fabric_token():
        return {"error": "Chưa đăng nhập Fabric hoặc phiên đã hết hạn — đăng nhập lại trước."}

    row_dim_id, col_dim_id = body["rowDim"], body["colDim"]
    if not _live_dim_supported(row_dim_id):
        return {"error": _live_dim_unsupported_msg(row_dim_id)}
    if not _live_dim_supported(col_dim_id):
        return {"error": _live_dim_unsupported_msg(col_dim_id)}

    rowval, colval = body["rowVal"], body["colVal"]
    metric = body.get("metric", "lr")
    s, e = body["start"], body["end"]
    mode = body.get("mode", "current")
    path = [{"dim": row_dim_id, "val": rowval}, {"dim": col_dim_id, "val": colval}]

    try:
        portfolio, _ = _live_portfolio(s, e)
        seg_metrics = _live_segment_metrics(path, s, e)
    except Exception as ex:  # noqa: BLE001
        return {"error": f"Truy vấn Live từ Fabric lỗi: {ex}"}
    portfolio["ae"] = 1.0
    _exp_inc = portfolio.get("freq", 0.0) * portfolio.get("sev", 0.0) * seg_metrics.get("exposure", 0.0)
    seg_metrics["ae"] = (seg_metrics.get("incurred", 0.0) / _exp_inc) if _exp_inc else 0.0

    prev_block = None
    if mode in ("yoy", "mom"):
        ps, pe = shift_period(s, e, mode)
        ps_s, pe_s = ps.date().isoformat(), pe.date().isoformat()
        try:
            prev_portfolio, _ = _live_portfolio(ps_s, pe_s)
            pseg_metrics = _live_segment_metrics(path, ps_s, pe_s)
        except Exception as ex:  # noqa: BLE001
            return {"error": f"Truy vấn Live kỳ so sánh từ Fabric lỗi: {ex}"}
        prev_portfolio["ae"] = 1.0
        _pexp_inc = prev_portfolio.get("freq", 0.0) * prev_portfolio.get("sev", 0.0) * pseg_metrics.get("exposure", 0.0)
        pseg_metrics["ae"] = (pseg_metrics.get("incurred", 0.0) / _pexp_inc) if _pexp_inc else 0.0
        prev_block = {
            "period": [ps_s, pe_s], "metrics": pseg_metrics, "portfolio": prev_portfolio,
            "bridgeVsPrev": safe_bridge(pseg_metrics, seg_metrics),
        }

    try:
        drivers = _live_scan_drivers(path, seg_metrics, metric, s, e, top=24)
    except Exception as ex:  # noqa: BLE001
        return {"error": f"Truy vấn Live từ Fabric lỗi (Khám phá nhanh): {ex}"}

    return {
        "segment": {"rowDim": row_dim_id, "rowVal": rowval, "colDim": col_dim_id, "colVal": colval},
        "metric": metric, "period": [s, e],
        "metrics": seg_metrics, "portfolio": portfolio,
        "bridgeVsPortfolio": safe_bridge(portfolio, seg_metrics),
        "prevPeriod": prev_block, "drivers": drivers,
    }


def h_fabric_live_decompose_detail(body):
    """Xem trước hồ sơ bồi thường trong đúng đoạn Decomposition Tree (Live) — tối đa 50 hồ sơ Incurred
    lớn nhất, kèm tổng số thật lấy từ chính seg_metrics (không cần thêm một lượt gọi Fabric đếm riêng)."""
    if not FABRIC:
        return {"error": "Máy này chưa nạp được kết nối Fabric."}
    if not fabric_token():
        return {"error": "Chưa đăng nhập Fabric hoặc phiên đã hết hạn — đăng nhập lại trước."}
    path = body.get("path", [])
    err = _live_path_error(path)
    if err:
        return {"error": err}
    s, e = body["start"], body["end"]
    try:
        seg_metrics = _live_segment_metrics(path, s, e)
        rows = _live_claims_rows(path, s, e, limit=50)
    except Exception as ex:  # noqa: BLE001
        return {"error": f"Truy vấn Live từ Fabric lỗi: {ex}"}
    return {"total": seg_metrics["claims"], "shown": len(rows), "rows": rows,
            "incurredTotal": seg_metrics["incurred"]}


def h_fabric_live_decompose_export(handler, token, qs):
    """GET nhị phân (xlsx) cho Live — cùng mẫu như h_decompose_export nhưng chỉ 5 cột cơ bản (Số hồ sơ/
    Số hợp đồng/Ngày xảy ra tổn thất/Incurred/TrangThai), KHÔNG kèm các cột chiều khác như bản kéo-toàn-
    bộ (mỗi chiều thêm sẽ cần một lượt RELATED() nặng riêng trong DAX cho từng dòng — không đáng đánh
    đổi tốc độ). Chặn xuất nếu đoạn quá rộng (> LIVE_CLAIMS_EXPORT_ROW_CAP dòng) để tránh lặp lại kiểu
    lỗi vượt giới hạn byte đã gặp ở bảng "DT kế toán" — báo người dùng thu hẹp path trước."""
    if not FABRIC or not fabric_token():
        handler._send_json({"error": "Chưa đăng nhập Fabric hoặc phiên đã hết hạn."}, code=400)
        return
    try:
        path = json.loads((qs.get("path") or ["[]"])[0])
        s, e = (qs.get("start") or [""])[0], (qs.get("end") or [""])[0]
        err = _live_path_error(path)
        if err:
            handler._send_json({"error": err}, code=400)
            return
        n = _live_claims_count(path, s, e)
        if n > LIVE_CLAIMS_EXPORT_ROW_CAP:
            handler._send_json({"error": f"Đoạn này có {n:,} hồ sơ, vượt giới hạn xuất Live "
                                          f"({LIVE_CLAIMS_EXPORT_ROW_CAP:,}) — hãy bóc tách hẹp hơn rồi xuất lại."},
                                code=400)
            return
        rows = _live_claims_rows(path, s, e, limit=None)
    except Exception as ex:  # noqa: BLE001
        handler._send_json({"error": f"Truy vấn Live từ Fabric lỗi: {ex}"}, code=400)
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "Ho so boi thuong"
    ws.append(["Số hồ sơ", "Số hợp đồng", "Ngày xảy ra tổn thất", "Incurred", "TrangThai"])
    for r in rows:
        ws.append([r["soHoSo"], r["soHopDong"], r["ngayTonThat"], r["incurred"], r["trangThai"]])
    buf = io.BytesIO()
    wb.save(buf)
    payload = buf.getvalue()
    handler.send_response(200)
    handler.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    handler.send_header("Content-Disposition", 'attachment; filename="decomposition_ho_so_live.xlsx"')
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def h_fabric_live_decompose(body):
    """Decomposition Tree cho Live Connection — trả đúng hình dạng như h_decompose (metrics/path/
    availableDims/splitDim/candidates) để dùng lại nguyên UI cây phân rã hiện có, chỉ đổi nguồn từ
    kéo-toàn-bộ+pandas sang truy vấn DAX theo đúng path đang đứng."""
    if not FABRIC:
        return {"error": "Máy này chưa nạp được kết nối Fabric."}
    if not fabric_token():
        return {"error": "Chưa đăng nhập Fabric hoặc phiên đã hết hạn — đăng nhập lại trước."}

    s, e = body["start"], body["end"]
    metric = body.get("metric", "lr")
    path = body.get("path", [])
    split_dim = body.get("splitDim")

    err = _live_path_error(path)
    if err:
        return {"error": err}
    if split_dim and not _live_dim_supported(split_dim):
        return {"error": _live_dim_unsupported_msg(split_dim)}

    try:
        portfolio, _ = _live_portfolio(s, e)
        seg_metrics = _live_segment_metrics(path, s, e)
    except Exception as ex:  # noqa: BLE001
        return {"error": f"Truy vấn Live từ Fabric lỗi: {ex}"}

    _pf, _ps = portfolio.get("freq", 0.0), portfolio.get("sev", 0.0)
    _exp_inc = _pf * _ps * seg_metrics.get("exposure", 0.0)
    seg_metrics["ae"] = (seg_metrics.get("incurred", 0.0) / _exp_inc) if _exp_inc else 0.0

    used_dims = {step["dim"] for step in path}
    available_dims = [d for d in LIVE_DIM_MAP if _live_dim_supported(d) and d not in used_dims]

    out = {"metrics": seg_metrics, "path": path, "availableDims": available_dims}
    if split_dim:
        out["splitDim"] = split_dim
        try:
            raw_candidates, _ = _live_split(path, split_dim, s, e)
        except Exception as ex:  # noqa: BLE001
            return {"error": f"Truy vấn Live từ Fabric lỗi: {ex}"}
        candidates = _live_candidates_from_raw(split_dim, raw_candidates, seg_metrics, metric, min_claims=1)
        candidates.sort(key=lambda c: -abs(c["impact"]))
        out["candidates"] = candidates[:200]
    return out


def h_dim_values(body):
    """Danh sách giá trị của một chiều trong đúng kỳ đang lọc — cấp dữ liệu cho ô chọn có tìm kiếm
    ở chế độ Đào sâu chi tiết. Không xếp hạng, chỉ liệt kê đủ để gõ-tìm."""
    if STORE["pol"] is None or STORE["clm"] is None:
        return {"error": "Chưa có dữ liệu đã nạp. Hãy hoàn tất Bước 1."}
    pol, clm = STORE["pol"], STORE["clm"]
    col = "_" + body["dim"]
    s, e = body["start"], body["end"]
    _portfolio, pl, cl = slice_metrics(pol, clm, s, e)
    vals = set()
    if col in pl.columns:
        vals |= set(pl[col].dropna().astype(str).unique())
    if col in cl.columns:
        vals |= set(cl[col].dropna().astype(str).unique())
    return {"values": order_dim_values(body["dim"], sorted(vals))}


def h_drilldown(body):
    if STORE["pol"] is None or STORE["clm"] is None:
        return {"error": "Chưa có dữ liệu đã nạp. Hãy hoàn tất Bước 1."}
    pol, clm = STORE["pol"], STORE["clm"]
    rowdim, coldim = "_" + body["rowDim"], "_" + body["colDim"]
    rowval, colval = body["rowVal"], body["colVal"]
    metric = body.get("metric", "lr")
    s, e = body["start"], body["end"]
    mode = body.get("mode", "current")

    _full_portfolio, pl, cl = slice_metrics(pol, clm, s, e)
    pl, cl, focused = _apply_focus(pl, cl, body)
    portfolio = EG.metrics(pl, cl) if focused else _full_portfolio
    seg_pl, seg_cl = segment_filter(pl, cl, rowdim, coldim, rowval, colval)
    seg_metrics = EG.metrics(seg_pl, seg_cl)
    # EG.metrics() không có khoá "ae" (A/E tính riêng theo từng ô ở cell_metrics/add_ae) — bổ sung ở
    # đây để KPI đào sâu không vỡ khi người dùng đang chọn A/E làm chỉ số. Nền toàn danh mục so với
    # chính nó luôn = 1,00 theo đúng định nghĩa (kỳ vọng = freq×sev toàn danh mục).
    portfolio["ae"] = 1.0
    _exp_inc = portfolio.get("freq", 0.0) * portfolio.get("sev", 0.0) * seg_metrics.get("exposure", 0.0)
    seg_metrics["ae"] = (seg_metrics.get("incurred", 0.0) / _exp_inc) if _exp_inc else 0.0

    prev_block = None
    if mode in ("yoy", "mom"):
        ps, pe = shift_period(s, e, mode)
        _full_pprev, ppl, pcl = slice_metrics(pol, clm, ps.date().isoformat(), pe.date().isoformat())
        ppl, pcl, _ = _apply_focus(ppl, pcl, body)
        prev_portfolio = EG.metrics(ppl, pcl) if focused else _full_pprev
        pseg_pl, pseg_cl = segment_filter(ppl, pcl, rowdim, coldim, rowval, colval)
        pseg_metrics = EG.metrics(pseg_pl, pseg_cl)
        prev_portfolio["ae"] = 1.0
        _pexp_inc = prev_portfolio.get("freq", 0.0) * prev_portfolio.get("sev", 0.0) * pseg_metrics.get("exposure", 0.0)
        pseg_metrics["ae"] = (pseg_metrics.get("incurred", 0.0) / _pexp_inc) if _pexp_inc else 0.0
        prev_block = {
            "period": [ps.date().isoformat(), pe.date().isoformat()],
            "metrics": pseg_metrics, "portfolio": prev_portfolio,
            "bridgeVsPrev": safe_bridge(pseg_metrics, seg_metrics),
        }

    exclude_cols = {rowdim, coldim}
    for step in (body.get("focusPath") or []):
        if step.get("dim"):
            exclude_cols.add("_" + step["dim"])
    drivers = scan_drivers(seg_pl, seg_cl, exclude_cols, seg_metrics, metric, top=24)

    return {
        "segment": {"rowDim": body["rowDim"], "rowVal": rowval, "colDim": body["colDim"], "colVal": colval},
        "metric": metric, "period": [s, e],
        "metrics": seg_metrics, "portfolio": portfolio,
        "bridgeVsPortfolio": safe_bridge(portfolio, seg_metrics),
        "prevPeriod": prev_block, "drivers": drivers,
    }


def h_decompose(body):
    """Decomposition Tree — bẻ dần một đoạn theo chuỗi chiều người dùng tự chọn từng bước, khác
    drilldown ở chỗ không cố định hai chiều (hàng/cột) mà cho drill xuống bao nhiêu chiều tuỳ ý.
    `path`: [{"dim","val"}, ...] tính TỪ ô heatmap gốc (đã gồm rowDim/rowVal, colDim/colVal nếu có)."""
    if STORE["pol"] is None or STORE["clm"] is None:
        return {"error": "Chưa có dữ liệu đã nạp. Hãy hoàn tất Bước 1."}
    pol, clm = STORE["pol"], STORE["clm"]
    s, e = body["start"], body["end"]
    metric = body.get("metric", "lr")
    path = body.get("path", [])
    split_dim = body.get("splitDim")

    _portfolio, pl, cl = slice_metrics(pol, clm, s, e)
    used_cols = set()
    for step in path:
        col = "_" + step["dim"]
        used_cols.add(col)
        if col in cl.columns:
            cl = cl[cl[col].astype(str) == step["val"]]
        if col in pl.columns and pl[col].notna().any():
            pl = pl[pl[col].astype(str) == step["val"]]
    seg_metrics = EG.metrics(pl, cl)
    # Cùng lý do như h_drilldown: EG.metrics() không có "ae". So với kỳ vọng của TOÀN DANH MỤC gốc
    # (trước khi theo path), nhất quán với cách drilldown đang so — không phải so với nút cha liền kề.
    _pf, _ps = _portfolio.get("freq", 0.0), _portfolio.get("sev", 0.0)
    _exp_inc = _pf * _ps * seg_metrics.get("exposure", 0.0)
    seg_metrics["ae"] = (seg_metrics.get("incurred", 0.0) / _exp_inc) if _exp_inc else 0.0

    out = {"metrics": seg_metrics, "path": path,
           "availableDims": [d[0] for d in DIM_DEFS if ("_" + d[0]) not in used_cols]}
    if split_dim:
        out["splitDim"] = split_dim
        out["candidates"] = split_by_dim(pl, cl, split_dim, seg_metrics, metric, top=200)
    return out


def _path_filter_claims(path, s, e):
    """Lọc bảng Bồi thường theo đúng chuỗi chiều+giá trị của Decomposition Tree — dùng chung cho preview
    và export Excel, để hai nơi luôn thấy đúng một tập hồ sơ."""
    pol, clm = STORE["pol"], STORE["clm"]
    _portfolio, _pl, cl = slice_metrics(pol, clm, s, e)
    for step in path or []:
        col = "_" + step["dim"]
        if col in cl.columns:
            cl = cl[cl[col].astype(str) == step["val"]]
    return cl


DECOMPOSE_DETAIL_COLS = ["Số hồ sơ", "Số hợp đồng", "Ngày xảy ra tổn thất", "Incurred", "TrangThai"]


def h_decompose_detail(body):
    """Xem trước các hồ sơ bồi thường nằm trong đúng đoạn Decomposition Tree hiện tại (root → nấc đang
    xem) — trả tối đa 50 hồ sơ Incurred lớn nhất kèm tổng số thật để người dùng biết còn ẩn bao nhiêu."""
    if STORE["pol"] is None or STORE["clm"] is None:
        return {"error": "Chưa có dữ liệu đã nạp. Hãy hoàn tất Bước 1."}
    cl = _path_filter_claims(body.get("path", []), body["start"], body["end"])
    total = len(cl)
    incurred_total = float(cl["Incurred"].sum()) if total else 0.0
    show = cl.sort_values("Incurred", ascending=False).head(50)
    rows = []
    for _, r in show.iterrows():
        rows.append({
            "soHoSo": str(r.get("Số hồ sơ", "")), "soHopDong": str(r.get("Số hợp đồng", "")),
            "ngayTonThat": str(r.get("Ngày xảy ra tổn thất", ""))[:10],
            "incurred": float(r.get("Incurred", 0) or 0), "trangThai": str(r.get("TrangThai", "")),
        })
    return {"total": total, "shown": len(rows), "rows": rows, "incurredTotal": incurred_total}


def h_decompose_export(handler, token, qs):
    """GET nhị phân (xlsx) — không qua _send_json vì trả file. Path truyền qua query string dạng JSON
    (không có phần thân POST khi trình duyệt tải file trực tiếp qua thẻ <a>/window.location).
"""
    if STORE["pol"] is None or STORE["clm"] is None:
        handler._send_json({"error": "Chưa có dữ liệu đã nạp."}, code=400)
        return
    try:
        path = json.loads((qs.get("path") or ["[]"])[0])
        s, e = (qs.get("start") or [""])[0], (qs.get("end") or [""])[0]
        cl = _path_filter_claims(path, s, e)
    except Exception as e:  # noqa: BLE001
        handler._send_json({"error": f"Tham số không hợp lệ: {e}"}, code=400)
        return

    dim_cols = [(label, "_" + dim_id) for dim_id, label, _csv, _pq in DIM_DEFS if ("_" + dim_id) in cl.columns]
    header = DECOMPOSE_DETAIL_COLS + [lb for lb, _ in dim_cols]
    wb = Workbook()
    ws = wb.active
    ws.title = "Ho so boi thuong"
    ws.append(header)
    for _, r in cl.iterrows():
        row = []
        for c in DECOMPOSE_DETAIL_COLS + [col for _, col in dim_cols]:
            v = r.get(c)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                row.append(None)
            elif hasattr(v, "isoformat"):
                row.append(v.isoformat())
            else:
                row.append(v)
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    payload = buf.getvalue()
    handler.send_response(200)
    handler.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    handler.send_header("Content-Disposition", 'attachment; filename="decomposition_ho_so.xlsx"')
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


# ------------------------------------------------------------ Microsoft Fabric (nguồn thứ tư)
# Device code flow tách hai bước (xem pipeline/fabric_extract.py): bước 1 khởi tạo và trả về
# ngay mã+link để hiện lên UI; bước 2 mới CHẶN chờ người dùng hoàn tất trên trình duyệt. Giữ
# app/flow/cache đang dở giữa hai request bằng biến module — chỉ một phiên đăng nhập Fabric
# chạy cùng lúc trên một server, đủ dùng cho mô hình một chủ đầu mối nạp dữ liệu.
def fabric_token():
    """Token Fabric của phiên đang xử lý; gọi ngoài phạm vi request (CLI) thì rơi về token cache file."""
    if not FABRIC:
        return None
    sess = WA.current()
    if sess is None:
        return FABRIC.get_cached_token()
    return WA.access_token(FABRIC, sess)


def fabric_account():
    if not FABRIC:
        return None
    sess = WA.current()
    if sess is None:
        return FABRIC.whoami()
    return WA.account(FABRIC, sess)


def h_fabric_last_refresh(_body):
    """Thời điểm dataset trên Fabric hoàn tất làm mới lần gần nhất — mốc "số liệu tới lúc nào" mà mọi
    truy vấn Live đang phản ánh. Dùng REST /refreshes của Power BI, cùng quyền Dataset.Read.All đã có."""
    if not FABRIC:
        return {"error": "Máy chủ chưa cài đặt kết nối Fabric."}
    tok = fabric_token()
    if not tok:
        return {"error": "Chưa đăng nhập."}
    import requests
    cfg = FABRIC.load_config()
    url = (f"https://api.powerbi.com/v1.0/myorg/groups/{cfg['workspaceId']}"
           f"/datasets/{cfg['datasetId']}/refreshes?$top=10")
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=60)
    except Exception as e:  # noqa: BLE001
        return {"error": f"Không gọi được Power BI: {e}"}
    if r.status_code != 200:
        return {"error": f"Power BI trả HTTP {r.status_code}"}
    for item in r.json().get("value", []):
        if item.get("status") == "Completed" and item.get("endTime"):
            return {"endTime": item["endTime"], "refreshType": item.get("refreshType")}
    return {"error": "Chưa có lần làm mới nào hoàn tất."}


def h_fabric_status(_body):
    if not FABRIC:
        return {"available": False}
    try:
        return {"available": True, "account": fabric_account(), "loginUrl": "/auth/login"}
    except Exception as e:  # noqa: BLE001
        return {"available": True, "configError": str(e)}


def h_fabric_login_start(_body):
    """Device code flow, chỉ mở khi chạy trên máy (chưa có redirect URI). Ghi vào token cache của phiên."""
    if not FABRIC:
        return {"error": "Máy này chưa cài đặt kết nối Fabric (thiếu fabric_extract.py hoặc msal)."}
    try:
        flow = WA.device_start(FABRIC, WA.current())
    except Exception as e:  # noqa: BLE001
        return {"error": f"Không khởi tạo được đăng nhập Fabric: {e}"}
    return {"userCode": flow["user_code"],
            "verificationUri": flow.get("verification_uri") or flow.get("verification_uri_complete", ""),
            "message": flow["message"], "expiresIn": flow.get("expires_in")}


def h_fabric_login_wait(_body):
    """CHẶN tới khi người dùng hoàn tất đăng nhập trên trình duyệt (hoặc hết hạn, ~15 phút của AAD) —
    an toàn vì ThreadingHTTPServer xử lý mỗi request trên một thread riêng, không chặn request khác."""
    try:
        acc = WA.device_wait(FABRIC, WA.current())
    except Exception as e:  # noqa: BLE001
        return {"error": f"Đăng nhập thất bại hoặc hết hạn: {e}"}
    return {"ok": True, "account": acc}


def h_fabric_logout(_body):
    if not FABRIC:
        return {"error": "Máy này chưa cài đặt kết nối Fabric."}
    WA.logout(FABRIC, WA.current())
    return {"ok": True}


def h_sqlserver_connect(_body):
    """Demo — form đủ trường để người xem hình dung luồng thật, nhưng CHƯA thật sự kết nối gì.
    Không log lại thông tin đăng nhập gửi lên (kể cả demo) — chỉ trả về thông báo cố định."""
    return {"error": "Tính năng đang xây dựng — Kết nối Data Warehouse SQL Server sẽ có ở bản sau (coming soon)."}


def h_fabric_pull(_body):
    """Kéo dữ liệu mới nhất từ Fabric: fabric_extract.py sinh dim_*/fact_*.parquet (như p1_extract.py
    làm với PBIX cục bộ), rồi gọi p2_prep.run() TRỰC TIẾP trong cùng tiến trình (không qua subprocess)
    để hoàn tất làm sạch/gắn chiều — không làm lại logic đó ở đây, đúng nguyên tắc một nguồn công thức
    duy nhất. Chạy trong cùng tiến trình (thay vì subprocess.run([sys.executable, "p2_prep.py"]) như
    trước) là điều kiện để tính năng này hoạt động được ở bản đóng gói .exe, nơi sys.executable là
    chính file .exe đã đóng băng chứ không phải một python.exe thật."""
    if not FABRIC:
        return {"error": "Máy này chưa cài đặt kết nối Fabric (thiếu fabric_extract.py hoặc msal)."}
    if not P2_PREP:
        return {"error": "Máy này chưa nạp được p2_prep.py — không thể hoàn tất làm sạch dữ liệu."}
    if not fabric_token():
        return {"error": "Chưa đăng nhập Fabric hoặc phiên đã hết hạn — đăng nhập lại trước."}
    # fabric_extract.py tự tính OUT mặc định theo vị trí file của chính nó, có thể khác DATA_DIR
    # (VD bản dev: HERE của module fabric_extract.py là app/, còn DATA_DIR là pipeline/data) — ép về
    # đúng DATA_DIR để p2_prep.py sắp đọc lại đúng chỗ FABRIC vừa ghi, không đọc nhầm dữ liệu cũ.
    FABRIC.OUT = DATA_DIR
    os.makedirs(DATA_DIR, exist_ok=True)
    log_parts = []
    buf = io.StringIO()
    try:
        import contextlib
        with contextlib.redirect_stdout(buf):
            FABRIC.extract_all()
    except Exception as e:  # noqa: BLE001
        return {"error": f"Trích xuất từ Fabric lỗi: {e}", "log": buf.getvalue()}
    log_parts.append(buf.getvalue())

    # extract_all() đã tự đánh dấu done=True khi xong — đặt lại "đang chạy" cho bước p2_prep.py để
    # progress bar phía frontend không hiện nhầm là đã xong trong lúc vẫn còn đang làm sạch dữ liệu.
    FABRIC._progress_set(phase="Đang làm sạch & gắn chiều dữ liệu (p2_prep.py)…", table="")
    try:
        p2_log = P2_PREP.run(DATA_DIR)
        log_parts.append("$ p2_prep\n" + "\n".join(p2_log[-200:]))
    except Exception as e:  # noqa: BLE001
        FABRIC._progress_set(phase="Lỗi ở bước làm sạch dữ liệu.")
        return {"error": f"p2_prep lỗi: {e}", "log": "\n\n".join(log_parts)}
    FABRIC._progress_set(phase="Hoàn tất — dữ liệu mới đã sẵn sàng.")

    result = h_validate({"source": "pipeline"})
    if "error" not in result:
        STORE["source"] = "fabric"
        result["log"] = "\n\n".join(log_parts)
    return result


def h_fabric_progress(_body):
    """Cho phía frontend polling trong lúc /api/fabric_pull vẫn đang chạy ở luồng khác — server dùng
    ThreadingHTTPServer nên request này phục vụ được song song, không phải chờ pull xong mới trả lời."""
    if not FABRIC or not hasattr(FABRIC, "progress_snapshot"):
        return {"error": "Chưa có dữ liệu tiến độ."}
    return FABRIC.progress_snapshot()


ROUTES = {"/api/validate": h_validate, "/api/analyze": h_analyze,
          "/api/drilldown": h_drilldown,
          "/api/fabric_status": h_fabric_status,
          "/api/fabric_last_refresh": h_fabric_last_refresh, "/api/fabric_login_start": h_fabric_login_start,
          "/api/fabric_login_wait": h_fabric_login_wait, "/api/fabric_pull": h_fabric_pull,
          "/api/fabric_progress": h_fabric_progress,
          "/api/fabric_live_analyze": h_fabric_live_analyze,
          "/api/fabric_live_decompose": h_fabric_live_decompose,
          "/api/fabric_live_drilldown": h_fabric_live_drilldown,
          "/api/fabric_live_decompose_detail": h_fabric_live_decompose_detail,
          "/api/fabric_logout": h_fabric_logout,
          "/api/dim_values": h_dim_values, "/api/decompose": h_decompose,
          "/api/decompose_detail": h_decompose_detail,
          "/api/sqlserver_connect": h_sqlserver_connect}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _send_json(self, obj, code=200):
        payload = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self._set_cookie()
        self.end_headers()
        self.wfile.write(payload)

    def _redirect(self, location):
        self.send_response(302)
        self.send_header("Location", location)
        self._set_cookie()
        self.end_headers()

    def _set_cookie(self):
        if getattr(self, "_sess_new", False):
            self.send_header("Set-Cookie", WA.cookie_header_value(self._sess))

    def _open_session(self):
        self._sess, self._sess_new = WA.get_or_create(self.headers.get("Cookie"))
        return WA.bind(self._sess)

    def _with_fabric_token(self, fn):
        """Đặt token Fabric của phiên vào ContextVar của fabric_extract trong lúc chạy fn."""
        tok = None
        if FABRIC and self._sess is not None:
            try:
                tok = WA.access_token(FABRIC, self._sess)
            except Exception:  # noqa: BLE001
                tok = None
        ctx_tok = FABRIC.SESSION_TOKEN.set(tok) if FABRIC else None
        try:
            return fn()
        finally:
            if ctx_tok is not None:
                FABRIC.SESSION_TOKEN.reset(ctx_tok)

    def do_GET(self):
        ctx = self._open_session()
        try:
            self._do_get()
        finally:
            if ctx is not None:
                WA.unbind(ctx)

    def _do_get(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        token = (qs.get("token") or [None])[0]

        if path in ("/", "/index.html"):
            with open(UI_FILE, "r", encoding="utf-8") as f:
                html = f.read().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            # Trang sửa liên tục trong lúc phát triển — chặn cache để F5 thường (không phải Ctrl+F5)
            # cũng luôn lấy đúng bản mới nhất, tránh nhầm "thiếu tính năng" vì trình duyệt giữ bản cũ.
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self._set_cookie()
            self.end_headers()
            self.wfile.write(html)
        elif path == "/auth/login":
            if not FABRIC:
                self._redirect(WA.error_redirect("Bản này không có đăng nhập Microsoft."))
                return
            try:
                self._redirect(WA.login_url(FABRIC, self._sess))
            except Exception as e:  # noqa: BLE001
                self._redirect(WA.error_redirect(f"Không khởi tạo được đăng nhập: {e}"))
        elif path == "/auth/callback":
            if not FABRIC:
                self._redirect("/")
                return
            params = {k: v[0] for k, v in qs.items()}
            try:
                WA.finish_login(FABRIC, self._sess, params)
                self._redirect("/")
            except Exception as e:  # noqa: BLE001
                self._redirect(WA.error_redirect(str(e)))
        elif path == "/auth/logout":
            if FABRIC and self._sess is not None:
                WA.logout(FABRIC, self._sess)
            self._redirect("/")
        elif path == "/api/status":
            try:
                account = fabric_account()
            except Exception:  # noqa: BLE001
                account = None
            out = {
                "standalone": STANDALONE, "account": account,
                "source": STORE["source"], "loadedAt": STORE["loadedAt"],
                "hasData": STORE["pol"] is not None and STORE["clm"] is not None,
                "pipelineDataExists": os.path.isfile(os.path.join(DATA_DIR, "clean_policy.parquet")),
                "fabricAvailable": FABRIC is not None,
            }
            if STORE.get("lastValidate"):
                out["lastValidate"] = STORE["lastValidate"]
            self._send_json(out)
        elif path == "/api/decompose_export":
            h_decompose_export(self, token, qs)
        elif path == "/api/fabric_live_decompose_export":
            self._with_fabric_token(lambda: h_fabric_live_decompose_export(self, token, qs))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        ctx = self._open_session()
        try:
            self._do_post()
        finally:
            if ctx is not None:
                WA.unbind(ctx)

    def _do_post(self):
        fn = ROUTES.get(self.path)
        if not fn:
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            self._send_json({"error": f"{type(e).__name__}: {e}"})
            return
        try:
            result = self._with_fabric_token(lambda: fn(body))
        except Exception as e:  # noqa: BLE001
            result = {"error": f"{type(e).__name__}: {e}"}
        self._send_json(result)


class ThreadingHTTPServer6(ThreadingHTTPServer):
    address_family = socket.AF_INET6


def run_local():
    """App chạy trên máy của từng người: chỉ nghe loopback, tự mở trình duyệt. Mỗi người đăng nhập
    bằng tài khoản Power BI của chính mình nên Fabric áp đúng phân quyền dữ liệu của họ."""
    # Windows phân giải "localhost" sang ::1 TRƯỚC 127.0.0.1. Chỉ nghe IPv4 thì mỗi request phải chờ
    # IPv6 thất bại rồi mới thử lại — đo được 2,05 giây/request so với 0,03 giây khi gọi thẳng
    # 127.0.0.1. Redirect URI đăng ký với Entra ID là localhost nên phải nghe cả hai, mỗi họ địa chỉ
    # một socket riêng, vẫn chỉ loopback: máy khác trong mạng không vào được.
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
        srv6 = ThreadingHTTPServer6(("::1", PORT), Handler)
    except OSError as e:
        print(f"Khong mo duoc cong {PORT} — co the app dang chay o mot cua so khac roi. Chi tiet: {e}",
              flush=True)
        if STANDALONE:
            input("Bam Enter de dong...")
        return
    threading.Thread(target=srv6.serve_forever, daemon=True).start()
    print("=" * 70, flush=True)
    print("DBV Analytics Engine", flush=True)
    print(f"  Mo trinh duyet tai: {WA.BASE_URL}/", flush=True)
    print("  DUNG dong cua so nay khi con dung app - dong lai la tat app.", flush=True)
    print("=" * 70, flush=True)
    threading.Timer(1.0, lambda: webbrowser.open(WA.BASE_URL + "/")).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run_local()
