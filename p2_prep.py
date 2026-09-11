"""Bước 2: Làm sạch + gắn chiều + xây bảng nền policy/claims."""
import pandas as pd, numpy as np

pd.set_option("display.width", 200)


def prep_dims(loai_xe, dia_ban, xe_pl, hang_xe, dai_ly, don_vi, nguon_dv, can_bo, loai_xe2, dong_co):
    """Chuẩn hoá các bảng chiều thô (drop_duplicates + set_index) thành dạng attach() dùng được ngay.
    Tách riêng khỏi run() để gọi được từ cả luồng file (run()) lẫn luồng Live (dims kéo trực tiếp từ
    Fabric qua fabric_extract.py, không qua file) — hai luồng dùng CHUNG một logic gắn chiều, không
    viết lại hai lần dễ lệch nhau."""
    loai_xe  = loai_xe.drop_duplicates("Mã loại xe").set_index("Mã loại xe")
    dia_ban  = dia_ban.drop_duplicates("Hai giá trị đầu biển số xe").set_index("Hai giá trị đầu biển số xe")
    xe_pl    = xe_pl.drop_duplicates("Mã xe").set_index("Mã xe")
    hang_xe  = hang_xe.drop_duplicates("Mã hãng xe").set_index("Mã hãng xe")
    dai_ly   = dai_ly.drop_duplicates("Mã đại lý").set_index("Mã đại lý")
    don_vi   = don_vi.drop_duplicates("Mã đơn vị").set_index("Mã đơn vị")
    nguon_dv = nguon_dv.drop_duplicates("Mã nguồn khai thác").set_index("Mã nguồn khai thác")
    can_bo   = can_bo.drop_duplicates("Mã cán bộ khai thác").set_index("Mã cán bộ khai thác")
    loai_xe2 = loai_xe2.drop_duplicates("Mã Loại xe").set_index("Mã Loại xe")
    # Bảng "Động cơ" đôi lúc rỗng 0 dòng ngay trong PBIX (đã gặp thực tế) — không để việc này chặn
    # cả pipeline, chỉ riêng DongCo rơi về "Không xác định" như mọi chiều claims-native khác khi thiếu cột.
    if "Mã động cơ" in dong_co.columns:
        dong_co = dong_co.drop_duplicates("Mã động cơ").set_index("Mã động cơ")
    else:
        dong_co = pd.DataFrame(columns=["Tên động cơ"])
    return dict(loai_xe=loai_xe, dia_ban=dia_ban, xe_pl=xe_pl, hang_xe=hang_xe, dai_ly=dai_ly,
                don_vi=don_vi, nguon_dv=nguon_dv, can_bo=can_bo, loai_xe2=loai_xe2, dong_co=dong_co)


def clean(pol_raw, cgq_raw, dgq_raw, dims, gara_raw=None, on_log=None):
    """Làm sạch + gắn chiều — logic THUẦN, không đọc/ghi file, dùng chung cho run() (đọc từ
    data/*.parquet) và luồng Live (dữ liệu thô vừa kéo trực tiếp từ Fabric theo đúng kỳ đang xem,
    không qua file). dims: dict trả về từ prep_dims(). gara_raw=None thì bỏ qua bước gắn phân nhóm
    xưởng (NhomGara/ChinhHang/LienKet/GaraTenTat về 'Không xác định'/copy GaraTen).
    Trả về (pol_clean, clm_clean, log_lines)."""
    log = []

    def p(m):
        if on_log:
            on_log(m)
        log.append(str(m))

    loai_xe, dia_ban, xe_pl = dims["loai_xe"], dims["dia_ban"], dims["xe_pl"]
    hang_xe, dai_ly, don_vi = dims["hang_xe"], dims["dai_ly"], dims["don_vi"]
    nguon_dv, can_bo, loai_xe2, dong_co = dims["nguon_dv"], dims["can_bo"], dims["loai_xe2"], dims["dong_co"]
    if "Mã động cơ" not in dong_co.columns:
        p("CANH BAO: bang 'Dong co' dang rong -> DongCo se ve 'Khong xac dinh'")

    def attach(df):
        """Gắn toàn bộ chiều phân tích vào một bảng fact."""
        k = df["Mã nhóm rủi ro"].astype(str)
        df["MucDichSD"]   = k.map(loai_xe["F2"]).fillna("Chưa phân loại")
        df["NhomXe_F1"]   = k.map(loai_xe["F1"]).fillna("Chưa phân loại")
        df["NhomXe_F0"]   = k.map(loai_xe["F0"]).fillna("Chưa phân loại")
        df["TenLoaiXe"]   = k.map(loai_xe["Tên loại xe"]).fillna("Chưa phân loại")
        df["FTNDS"]       = k.map(loai_xe["FTNDS"]).fillna("Chưa phân loại")

        b = df["Hai giá trị đầu biển số xe"].astype(str)
        df["Tinh"]        = b.map(dia_ban["Địa bàn (theo mã biển số)"]).fillna("Không xác định")
        df["KhuVuc"]      = b.map(dia_ban["Phân nhóm khu vực của địa bàn"]).fillna("Không xác định")
        df["NhomRR_DiaBan"] = b.map(dia_ban["Phân nhóm rủi ro từng địa bàn"]).fillna("Không xác định")

        x = df["Mã xe"].astype(str)
        df["NhienLieu"]   = x.map(xe_pl["Nhiên liệu"]).fillna("Không phân loại")
        df["PhanKhuc"]    = x.map(xe_pl["Phân khúc"]).fillna("Không phân loại")
        df["KieuThanXe"]  = x.map(xe_pl["Kiểu thân xe"]).fillna("Không phân loại")
        df["DongXe"]      = x.map(xe_pl["Dòng xe"]).fillna("Không phân loại")
        df["HangXeSach"]  = x.map(xe_pl["Hãng xe làm sạch"]).fillna("Không phân loại")
        df["SoChoNgoi"]   = x.map(xe_pl["Số chỗ ngồi"]).fillna("Không phân loại")

        # Hãng xe: lấy từ bảng 'Hãng xe' cột 'Tên hãng xe' (nguồn chuẩn của nghiệp vụ).
        # Chuẩn hoá hoa/thường vì nguồn ghi lẫn lộn (VD "Vinfast" vs "TOYOTA").
        _hx = df["Mã hãng xe"].astype(str).map(hang_xe["Tên hãng xe"]).fillna(df["Mã hãng xe"].astype(str))
        _hx = _hx.astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)
        df["HangXe"] = _hx.replace({"NAN": "Không xác định", "NONE": "Không xác định", "": "Không xác định"})
        df["TenDaiLy"]    = df["Mã đại lý"].astype(str).map(dai_ly["Tên đại lý"]).fillna("Không xác định")
        df["NhomDaiLy"]   = df["Mã đại lý"].astype(str).map(dai_ly["Nhóm đại lý"]).fillna("Không xác định")

        dv = df["Ma_DV"].astype(str)
        df["DonVi"]       = dv.map(don_vi["ĐƠN VỊ"]).fillna("Không xác định")
        df["KhuVucDV"]    = dv.map(don_vi["Khu vực"]).fillna("Không xác định")
        df["QuyMoDV"]     = dv.map(don_vi["Nhóm quy mô"]).fillna("Không xác định")

        df["Kenh"]        = df["Kênh khai thác"].astype(str).replace({"nan": "Không xác định", "None": "Không xác định"})
        df["DaiGiaTriXe"] = df["Nhóm giá trị xe"].astype(str).replace({"nan": "Không xác định", "None": "Không xác định"})

        # --- chiều phân tích bổ sung cho app (Đào sâu chi tiết) ---
        cb = df["Mã cán bộ khai thác"].astype(str)
        df["CanBoKT"]     = cb.map(can_bo["Tên cán bộ khai thác"]).fillna("Không xác định")

        nd = df["Mã nguồn khai thác"].astype(str)
        df["DoiTac"]      = nd.map(nguon_dv["Nhóm nguồn khai thác"]).fillna("Không xác định")
        df["DiemBan"]     = nd.map(nguon_dv["Tên tắt"]).fillna("Không xác định")

        # "1. Mã loại xe" chỉ có ở phía Hợp đồng — Bồi thường nhận lại qua UNIFY (nối theo Số hợp đồng).
        # LƯU Ý: Series.map() trên một Series TOÀN NaN không trả NaN cho từng dòng như tưởng — nó trả về
        # giá trị đầu tiên của bảng đích lặp lại cho mọi dòng (đã đo thật, không phải suy đoán). Vì vậy khi
        # thiếu cột nguồn phải gán thẳng "Không xác định", TUYỆT ĐỐI không gọi .map() trên Series rỗng.
        if "1. Mã loại xe" in df.columns:
            lx2 = df["1. Mã loại xe"].astype(str)
            df["MDSD2"]         = lx2.map(loai_xe2["MDSD"]).fillna("Không xác định")
            df["SoChoDen"]      = lx2.map(loai_xe2["Số chỗ (đến)"]).fillna("Không xác định")
            df["TrongTaiKgDen"] = lx2.map(loai_xe2["Trọng tải kg (đến)"]).fillna("Không xác định")
        else:
            df["MDSD2"] = "Không xác định"
            df["SoChoDen"] = "Không xác định"
            df["TrongTaiKgDen"] = "Không xác định"

        df["PhongKD"]     = df.get("Tên Phòng ban", pd.Series(np.nan, index=df.index)) \
            .astype(str).replace({"nan": "Không xác định", "None": "Không xác định", "": "Không xác định"})
        return df

    # ---------------- POLICY ----------------
    pol = pol_raw.copy()
    n0 = len(pol)
    pol["dur"] = (pol["Ngày kết thúc hiệu lực"] - pol["Ngày hiệu lực hợp đồng"]).dt.days + 1

    bad_dur  = (pol["dur"] <= 0) | (pol["dur"] > 1100)
    bad_date = (pol["Ngày hiệu lực hợp đồng"] < "2019-01-01") | (pol["Ngày hiệu lực hợp đồng"] > "2027-12-31")
    # GIỮ LẠI bút toán điều chỉnh/huỷ (E0/CAN) và chỉ gắn cờ:
    #   measure 'Phí thực hưởng' tính cả chúng vào mẫu số (phí âm -> giảm phí thực hưởng),
    #   measure 'Đếm số hợp đồng' mới loại chúng khỏi exposure.
    pol["la_dieu_chinh"] = pol["Số hợp đồng"].str.contains("E0|CAN", na=False)
    p(f"POLICY tho {n0:,} dong | loai: thoi han bat thuong {bad_dur.sum():,} | ngay HL bat thuong {bad_date.sum():,}")
    p(f"   but toan dieu chinh/huy (E0/CAN) GIU LAI de tinh phi: {pol['la_dieu_chinh'].sum():,} dong")
    pol = pol[~(bad_dur | bad_date)].copy()
    p(f"-> con {len(pol):,} dong ({100*len(pol)/n0:.1f}%)")

    # gộp về mức hợp đồng (1 HĐ = 1 xe với VCX)
    agg = {
        "Phí BH phân bổ": "sum", "Hoa hồng BH gốc phân bổ": "sum",
        "Số tiền bảo hiểm của đối tượng bảo hiểm": "max", "Giá trị của đối tượng bảo hiểm": "max",
    }
    first_cols = ["la_dieu_chinh", "Mã khách hàng", "Tên khách hàng", "Mã nhóm rủi ro", "Nhóm giá trị xe", "Mã xe", "Mã hãng xe",
                  "Hiệu xe", "Hai giá trị đầu biển số xe", "Biển số xe", "Kênh khai thác", "Mã đại lý",
                  "Mã cán bộ khai thác", "Mã nguồn khai thác", "Ma_DV", "Loại Khách Hàng", "Mục đích sử dụng",
                  "Nhóm Thời gian sử dụng xe", "Thời gian sử dụng xe", "Năm sản xuất", "Ngày kế toán", "dur",
                  "Tên Phòng ban", "1. Mã loại xe"]
    # Chỉ thêm cột nào THỰC SỰ có mặt — nguồn Fabric giờ kéo ít cột hơn PBIX (đã rút bớt cột không ai
    # dùng để đỡ tốn thời gian mỗi lượt gọi API), nên vẫn phải chịu được cả hai trường hợp thiếu/đủ cột.
    for c in first_cols:
        if c in pol.columns:
            agg[c] = "first"
    pol = pol.groupby(["Số hợp đồng", "Ngày hiệu lực hợp đồng", "Ngày kết thúc hiệu lực"], as_index=False).agg(agg)
    p(f"-> gop ve muc hop dong: {len(pol):,} hop dong")
    pol = attach(pol)
    pol["tong_ngay"] = (pol["Ngày kết thúc hiệu lực"] - pol["Ngày hiệu lực hợp đồng"]).dt.days

    # ---------------- CLAIMS ----------------
    cgq = cgq_raw.copy()
    dgq = dgq_raw.copy()

    cgq["Incurred"] = cgq["Số tiền tổn thất phân bổ"].fillna(0) + cgq["Phí giám định phân bổ"].fillna(0)
    cgq["TrangThai"] = "Chưa giải quyết"
    cgq["NgayGiaiQuyet"] = pd.NaT
    dgq["Incurred"] = dgq["Số tiền bồi thường phân bổ"].fillna(0) + dgq["Phí giám định phân bổ"].fillna(0)
    dgq["TrangThai"] = "Đã giải quyết"
    dgq["NgayGiaiQuyet"] = dgq["Ngày giải quyết"]

    common = ["Mã NV", "Số hồ sơ", "Số hợp đồng", "Mã khách hàng", "Tên khách hàng", "Ngày xảy ra tổn thất",
              "Ngày thông báo", "Ngày mở HSBT", "Ngày hiệu lực hợp đồng", "NgayGiaiQuyet", "Incurred",
              "C_BOI_THUONG_THU_DOI_TBH", "Nguyên nhân tổn thất", "Sự kiện", "Mã nhóm rủi ro", "Nhóm giá trị xe",
              "Mã xe", "Mã hãng xe", "Hiệu xe", "Hai giá trị đầu biển số xe", "Kênh khai thác", "Mã đại lý",
              "Mã cán bộ khai thác", "Mã cán bộ bồi thường", "Mã nguồn khai thác", "Ma_DV", "Mã đơn vị xử lý",
              "TrangThai", "Phân loại số tiền tổn thất", "Mã động cơ"]
    cgq["SoNgayTon"] = cgq.get("Số ngày tồn", np.nan)
    dgq["SoNgayTon"] = np.nan
    for df in (cgq, dgq):
        for c in ("Phân loại số tiền tổn thất", "Mã động cơ"):
            if c not in df.columns:
                df[c] = np.nan

    # --- XƯỞNG SỬA CHỮA & CẤU PHẦN CHI PHÍ ---
    # Hai bảng đặt tên cột khác nhau nên phải hợp nhất trước. DGQ có hai cột mã xưởng
    # (xưởng ghi trên hồ sơ và xưởng thực xử lý); lấy cột đầu, thiếu thì lấy cột sau.
    cgq["GaraMa"] = cgq.get("Gara sửa chữa", pd.Series(np.nan, index=cgq.index))
    dgq["GaraMa"] = dgq.get("Mã Gara sửa chữa", pd.Series(np.nan, index=dgq.index))
    dgq["GaraMa"] = dgq["GaraMa"].fillna(dgq.get("Mã gara sửa chữa xử lý", np.nan))
    for df in (cgq, dgq):
        df["GaraTen"] = df.get("Tên Gara sửa chữa", pd.Series(np.nan, index=df.index))
        # ba cấu phần nằm ở MỨC HỒ SƠ, không phân bổ — lặp y hệt qua các dòng của cùng hồ sơ.
        # Giữ nguyên trạng ở đây; mọi phép cộng phải gộp về hồ sơ trước (xem p13_gara.py).
        for src, dst in [("Chi phí Sơn", "CP_Son"), ("Chi phí nhân công", "CP_NhanCong"),
                         ("Phí phụ tùng", "CP_PhuTung")]:
            df[dst] = pd.to_numeric(df.get(src, np.nan), errors="coerce")
        df["NguyenNhanChinh"] = df.get("Nguyên nhân chính", pd.Series(np.nan, index=df.index))
        df["NguyenNhanPhu"] = df.get("Nguyên nhân phụ", pd.Series(np.nan, index=df.index))
        df["MaNNChinh"] = df.get("Mã nguyên nhân chính", pd.Series(np.nan, index=df.index))

    GARA_C = ["GaraMa", "GaraTen", "CP_Son", "CP_NhanCong", "CP_PhuTung",
              "NguyenNhanChinh", "NguyenNhanPhu", "MaNNChinh"]
    clm = pd.concat([cgq[common + ["SoNgayTon"] + GARA_C], dgq[common + ["SoNgayTon"] + GARA_C]],
                    ignore_index=True)
    n0 = len(clm)

    bad = clm["Ngày xảy ra tổn thất"].isna() | (clm["Ngày xảy ra tổn thất"] < "2019-01-01") | (clm["Ngày xảy ra tổn thất"] > "2026-12-31")
    p(f"CLAIMS tho {n0:,} | ngay ton that bat thuong/trong: {bad.sum():,}")
    clm = clm[~bad].copy()
    clm = attach(clm)

    # --- Chiều CHỈ CÓ Ở PHÍA BỒI THƯỜNG (claims-native) — không gắn trong attach() vì hợp đồng
    # không có khái niệm "mức độ tổn thất" hay "động cơ ghi trên hồ sơ" khi chưa từng có claim.
    # Để trống (NaN) bên Hợp đồng, app tự nhận biết đây là chiều claims-native qua đó.
    clm["NhomMucDoTT"] = clm["Phân loại số tiền tổn thất"].astype(str).replace(
        {"nan": "Không xác định", "None": "Không xác định", "": "Không xác định"})
    _dc = clm["Mã động cơ"].astype(str)
    clm["DongCo"] = _dc.map(dong_co["Tên động cơ"]).fillna("Không xác định")

    # --- ĐỒNG BỘ CHIỀU: lấy hợp đồng làm nguồn chuẩn, nối qua Số hợp đồng ---
    # Bắt buộc, vì phía bồi thường thiếu tag ở nhiều trường (vd Nhóm giá trị xe trống 23%),
    # gây lệch tử số/mẫu số khi tính Frequency & Loss Ratio theo chiều.
    UNIFY = ["MucDichSD", "NhomXe_F1", "NhomXe_F0", "TenLoaiXe", "FTNDS", "Tinh", "KhuVuc", "NhomRR_DiaBan",
             "NhienLieu", "PhanKhuc", "KieuThanXe", "DongXe", "HangXeSach", "SoChoNgoi", "HangXe",
             "TenDaiLy", "NhomDaiLy", "DonVi", "KhuVucDV", "QuyMoDV", "Kenh", "DaiGiaTriXe",
             "CanBoKT", "DoiTac", "DiemBan", "PhongKD", "MDSD2", "SoChoDen", "TrongTaiKgDen"]
    POL_ONLY = ["Loại Khách Hàng", "Mục đích sử dụng", "Nhóm Thời gian sử dụng xe", "Năm sản xuất",
                "Số tiền bảo hiểm của đối tượng bảo hiểm", "Giá trị của đối tượng bảo hiểm", "Phí BH phân bổ"]
    lk = pol.drop_duplicates("Số hợp đồng").set_index("Số hợp đồng")
    hit = clm["Số hợp đồng"].isin(lk.index)
    p(f"Doi chieu bồi thường -> hợp đồng: khop {hit.mean():.1%} dong")
    for c in UNIFY:
        src = clm["Số hợp đồng"].map(lk[c])
        own = clm[c].astype(str).replace({"nan": np.nan, "None": np.nan, "": np.nan,
                                          "Không phân loại": np.nan, "Không xác định": np.nan})
        clm[c] = src.fillna(own).fillna("Không xác định")
    for c in POL_ONLY:
        clm[c + "_pol"] = clm["Số hợp đồng"].map(lk[c])
    for c in ["Loại Khách Hàng", "Mục đích sử dụng", "Nhóm Thời gian sử dụng xe"]:
        clm[c] = clm[c + "_pol"].astype(str).replace({"nan": "Không xác định", "None": "Không xác định"})
        pol[c] = pol[c].astype(str).replace({"nan": "Không xác định", "None": "Không xác định"})

    # --- Gắn phân nhóm xưởng từ bảng Gara sửa chữa ---
    # Nối theo tầng: mã quản lý -> tên đăng ký kinh doanh -> mã số thuế. Chuẩn hoá chữ hoa và
    # khoảng trắng trước khi so, nếu không tỷ lệ khớp tụt từ 93% xuống dưới 50%.
    try:
        if gara_raw is None:
            raise ValueError("khong co du lieu Gara (gara_raw=None)")
        _g = gara_raw.copy()
        for c in _g.columns:
            _g[c] = _g[c].astype(str).str.strip()
        _up = lambda x: x.astype(str).str.strip().str.upper()
        _nm = lambda x: (x.astype(str).str.upper()
                         .str.replace(r"[^0-9A-ZÀ-ỹ]+", " ", regex=True).str.strip())
        _k1 = dict(zip(_up(_g["MAQUANLY"]), _g["Phân nhóm SH/GR"]))
        _k2 = dict(zip(_nm(_g["TENDANGKYKD"]), _g["Phân nhóm SH/GR"]))
        _k3 = dict(zip(_up(_g["MASOTHUE"]), _g["Phân nhóm SH/GR"]))
        _a, _t = _up(clm["GaraMa"]), _nm(clm["GaraTen"])
        clm["NhomGara"] = _a.map(_k1).fillna(_t.map(_k2)).fillna(_a.map(_k3))
        _tenmap = dict(zip(_up(_g["MAQUANLY"]), _g["TENTHUONGGOI_TEN_TAT"]))
        clm["GaraTenTat"] = _a.map(_tenmap).fillna(clm["GaraTen"])
        clm["ChinhHang"] = np.where(clm["NhomGara"].astype(str).str.contains("Chính hãng"), "Chính hãng",
                            np.where(clm["NhomGara"].astype(str).str.contains("Gara ngoài"), "Gara ngoài",
                                     "Không xác định"))
        clm["LienKet"] = np.where(clm["NhomGara"].astype(str).str.contains("Có liên kết"), "Có liên kết",
                          np.where(clm["NhomGara"].astype(str).str.contains("Không liên kết"), "Không liên kết",
                                   "Không xác định"))
        clm["NhomGara"] = clm["NhomGara"].fillna("Không xác định")
        p(f"Gan phan nhom xuong: khop {clm['NhomGara'].ne('Không xác định').mean():.1%} dong")
    except Exception as e:
        p(f"Khong gan duoc phan nhom xuong: {e}")
        clm["NhomGara"] = "Không xác định"; clm["ChinhHang"] = "Không xác định"
        clm["LienKet"] = "Không xác định"; clm["GaraTenTat"] = clm["GaraTen"]

    clm["NamTT"] = clm["Ngày xảy ra tổn thất"].dt.year
    clm["ThangTT"] = clm["Ngày xảy ra tổn thất"].dt.to_period("M").astype(str)
    clm["DoTreKhaiBao"] = (clm["Ngày thông báo"] - clm["Ngày xảy ra tổn thất"]).dt.days
    clm["NgayTuHL"] = (clm["Ngày xảy ra tổn thất"] - clm["Ngày hiệu lực hợp đồng"]).dt.days
    clm["ThoiGianXuLy"] = (clm["NgayGiaiQuyet"] - clm["Ngày mở HSBT"]).dt.days
    clm["LaCAT"] = clm["Sự kiện"].notna() & (~clm["Sự kiện"].astype(str).isin(["nan", "None", "", "Không"]))
    p(f"-> CLAIMS sach: {len(clm):,} dong | {clm['Số hồ sơ'].nunique():,} ho so")
    p(f"   trong do CAT (co Su kien): {clm['LaCAT'].sum():,} dong")
    p(f"   Incurred tong: {clm['Incurred'].sum()/1e9:,.0f} ty")
    return pol, clm, log


def run(data_dir="data"):
    """Bản đọc/ghi file — dùng cho bản dev/đóng gói (PBIX hoặc Fabric-kéo-toàn-bộ). Đọc dim_*/fact_*.parquet
    trong data_dir, gọi prep_dims()+clean(), ghi clean_policy/clean_claims.parquet ra CHÍNH data_dir.
    Trả về list các dòng log. Gói thành hàm (thay vì script top-level chạy khi import) để app/server.py
    gọi được TRỰC TIẾP trong cùng tiến trình — cần cho bản đóng gói .exe, nơi sys.executable là chính
    file .exe đã đóng băng chứ không phải một python.exe thật, nên
    subprocess.run([sys.executable, "p2_prep.py"]) không chạy được."""
    D = data_dir

    def rd(name):
        return pd.read_parquet(f"{D}/{name}.parquet")

    dims = prep_dims(rd("dim_loai_xe"), rd("dim_dia_ban"), rd("dim_xe_phanloai"), rd("dim_hang_xe"),
                      rd("dim_dai_ly"), rd("dim_don_vi"), rd("dim_nguon_dv"), rd("dim_can_bo"),
                      rd("dim_loai_xe2"), rd("dim_dong_co"))
    try:
        gara_raw = rd("dim_gara")
    except Exception:
        gara_raw = None

    log = []

    def on_log(m):
        print(m, flush=True)

    pol, clm, sub_log = clean(rd("fact_policy"), rd("fact_cgq"), rd("fact_dgq"), dims,
                               gara_raw=gara_raw, on_log=on_log)
    log.extend(sub_log)
    pol.to_parquet(f"{D}/clean_policy.parquet", index=False)
    clm.to_parquet(f"{D}/clean_claims.parquet", index=False)

    on_log("\n--- Kiem tra nhanh theo nam ton that ---")
    g = clm[clm.NamTT.between(2024, 2026)].groupby("NamTT").agg(
        hoso=("Số hồ sơ", "nunique"), incurred=("Incurred", "sum"))
    g["incurred_ty"] = (g.incurred / 1e9).round(1)
    msg = g[["hoso", "incurred_ty"]].to_string()
    on_log(msg)
    log.append(msg)
    open(f"{D}/_prep_log.txt", "w", encoding="utf-8").write("\n".join(log))
    return log


if __name__ == "__main__":
    run()
