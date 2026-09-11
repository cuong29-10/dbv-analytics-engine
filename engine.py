"""Lõi tính toán của engine phân tích TLBT — bám đúng bộ công thức trong Motor Insurance Workbook.

KHÁC BIỆT DUY NHẤT so với pipeline báo cáo: nền đếm ngày đã SỬA.
    Báo cáo tĩnh  : DATEDIFF không cộng 1 (bám nguyên measure Power BI)
    Engine này    : số ngày của [a,b] = b − a + 1  (đúng Workbook §4.1)
Lý do: engine cho người dùng cắt kỳ tuỳ ý. Với nền cũ, TLBT H1/2026 chạy từ 66,19%
(cắt một lần) tới 77,36% (cộng 27 lát tuần) — thuần tuý do cách chia kỳ. Nền đã sửa
cộng dồn khớp tuyệt đối nên mọi lát cắt cho cùng một con số.

Hàm ở đây đều thuần: nhận (bảng, kỳ, bộ lọc) trả về số. Không đọc/ghi file, không in.
"""
import numpy as np
import pandas as pd

# ---------------------------------------------------------------- nền cơ sở

def add_days(pol):
    """Số ngày hiệu lực của hợp đồng, tính cả hai đầu mút (Workbook §4.1)."""
    pol = pol.copy()
    pol["tong_ngay"] = (pol["Ngày kết thúc hiệu lực"] - pol["Ngày hiệu lực hợp đồng"]).dt.days + 1
    return pol


def slice_pol(pol, s, e):
    """Exposure (xe-năm) và Earned (phí thực hưởng) của kỳ [s,e], cơ sở 1/365.

    Giữ hai đặc tính đã chốt của mô hình DBV:
      · Earned  tính CẢ bút toán điều chỉnh/huỷ E0/CAN (phí âm nằm trong mẫu số)
      · Exposure LOẠI E0/CAN
    Chỉ nền đếm ngày là khác: cộng 1 ở cả tử lẫn mẫu.
    """
    S, E = pd.Timestamp(s), pd.Timestamp(e)
    ov = (pol["Ngày kết thúc hiệu lực"].clip(upper=E)
          - pol["Ngày hiệu lực hợp đồng"].clip(lower=S)).dt.days + 1
    ov = ov.clip(lower=0)
    tot = pol["tong_ngay"]
    ok = (ov > 0) & (tot > 0)
    o = pol.copy()
    o["Earned"] = np.where(ok, pol["Phí BH phân bổ"] * ov / tot.replace(0, np.nan), 0.0)
    o["Exposure"] = np.where(ok & (~pol["la_dieu_chinh"]), ov / 365.0, 0.0)
    return o[ok]


def slice_clm(clm, s, e):
    return clm[(clm["Ngày xảy ra tổn thất"] >= pd.Timestamp(s))
               & (clm["Ngày xảy ra tổn thất"] <= pd.Timestamp(e))]


def metrics(pl, cl, dev=1.0):
    """Bộ chỉ tiêu nền. dev = hệ số phát triển dự phòng, chỉ áp lên phần đang dự phòng."""
    ex, ep = float(pl.Exposure.sum()), float(pl.Earned.sum())
    n = int(cl["Số hồ sơ"].nunique())
    inc = float(cl.Incurred.sum())
    os_ = float(cl.loc[cl.TrangThai == "Chưa giải quyết", "Incurred"].sum())
    inc_a = inc + os_ * (dev - 1)
    return dict(exposure=ex, earned=ep, claims=n, incurred=inc_a, incurred_raw=inc, os=os_,
                freq=n / ex if ex else 0.0, sev=inc_a / n if n else 0.0,
                pp=inc_a / ex if ex else 0.0, avgprem=ep / ex if ex else 0.0,
                lr=inc_a / ep if ep else 0.0)


# ------------------------------------------------------- phân rã (Workbook §4.2–4.5)

def tier1(m0, m1):
    """Tầng 1 — Rủi ro thực hay Định phí. Tổng hai hiệu ứng = ΔLR đúng tuyệt đối."""
    pp_eff = (m1["pp"] - m0["pp"]) / m0["avgprem"]
    ap_eff = m1["pp"] / m1["avgprem"] - m1["pp"] / m0["avgprem"]
    return dict(pp=pp_eff, ap=ap_eff, tong=pp_eff + ap_eff, thuc=m1["lr"] - m0["lr"])


def tier2(m0, m1):
    """Tầng 2 — Tần suất hay Chi phí mỗi vụ. Tổng = ΔPP."""
    f_eff = (m1["freq"] - m0["freq"]) * m0["sev"]
    s_eff = m1["freq"] * (m1["sev"] - m0["sev"])
    return dict(f=f_eff, s=s_eff, tong=f_eff + s_eff, thuc=m1["pp"] - m0["pp"])


def bridge(m0, m1):
    """Bridge — thay biến theo thứ tự cố định F → S → AP (Workbook §4.4)."""
    lr0 = m0["freq"] * m0["sev"] / m0["avgprem"]
    lr_f = m1["freq"] * m0["sev"] / m0["avgprem"]
    lr_fs = m1["freq"] * m1["sev"] / m0["avgprem"]
    lr1 = m1["freq"] * m1["sev"] / m1["avgprem"]
    return dict(lr0=lr0, dF=lr_f - lr0, dS=lr_fs - lr_f, dAP=lr1 - lr_fs, lr1=lr1)


def mix_within(p0, c0, p1, c1, dim):
    """Hiệu ứng cơ cấu Mix / Within theo trọng số phí (Workbook §4.5).

    Xử lý tường minh phân khúc SINH MỚI và BIẾN MẤT — nếu bỏ qua, trọng số không cộng
    về 1 và tổng đóng góp lệch hẳn khỏi ΔLR thật (đã đo: chiều Đơn vị ra −11,7 điểm
    trong khi mức tăng thật là +8,3 điểm).
    """
    a = pd.DataFrame({"ep0": p0.groupby(dim, observed=True).Earned.sum(),
                      "inc0": c0.groupby(dim, observed=True).Incurred.sum(),
                      "n0": c0.groupby(dim, observed=True)["Số hồ sơ"].nunique(),
                      "ex0": p0.groupby(dim, observed=True).Exposure.sum()})
    b = pd.DataFrame({"ep1": p1.groupby(dim, observed=True).Earned.sum(),
                      "inc1": c1.groupby(dim, observed=True).Incurred.sum(),
                      "n1": c1.groupby(dim, observed=True)["Số hồ sơ"].nunique(),
                      "ex1": p1.groupby(dim, observed=True).Exposure.sum()})
    d = a.join(b, how="outer").fillna(0.0)

    EP0, EP1 = float(p0.Earned.sum()), float(p1.Earned.sum())
    LR0, LR1 = float(c0.Incurred.sum()) / EP0, float(c1.Incurred.sum()) / EP1

    d["w0"], d["w1"] = d.ep0 / EP0, d.ep1 / EP1
    both = (d.ep0 > 0) & (d.ep1 > 0)
    d["trangthai"] = np.where(both, "co",
                       np.where(d.ep1 > 0, "moi", np.where(d.ep0 > 0, "mat", "khongphi")))
    d["lr0"] = np.where(d.ep0 > 0, d.inc0 / d.ep0.replace(0, np.nan), 0.0)
    d["lr1"] = np.where(d.ep1 > 0, d.inc1 / d.ep1.replace(0, np.nan), 0.0)

    # Đóng góp thật của mỗi nhóm vào ΔLR — đúng theo định nghĩa, cộng lại luôn khớp:
    #     tong_g = inc1_g/EP1 − inc0_g/EP0
    # Bốn thành phần bên dưới chỉ là cách CHIA con số đó, phần dư luôn được ghi nhận
    # tường minh chứ không bị bỏ rơi.
    d["tong"] = d.inc1 / EP1 - d.inc0 / EP0
    d["mix"] = np.where(both, (d.w1 - d.w0) * (d.lr0 - LR0), 0.0)
    d["within"] = np.where(both, d.w1 * (d.lr1 - d.lr0), 0.0)
    # Phân khúc sinh mới / biến mất giữa hai kỳ — danh mục DBV tăng gần gấp đôi nên
    # đây là phần lớn, bỏ qua là tổng lệch hẳn khỏi ΔLR thật.
    d["chuyen"] = np.where(d.trangthai.isin(["moi", "mat"]), d.tong, 0.0) \
        + np.where(both, d.tong - d.mix - d.within, 0.0)
    # Tổn thất có mà không quy được về phân khúc nào (chiều bị thiếu ở phía bồi thường).
    d["khongphi"] = np.where(d.trangthai == "khongphi", d.tong, 0.0)

    recon = dict(tong=float(d.tong.sum()), thuc=LR1 - LR0,
                 lech=float(d.tong.sum()) - (LR1 - LR0),
                 mix=float(d.mix.sum()), within=float(d.within.sum()),
                 chuyen=float(d.chuyen.sum()), khongphi=float(d.khongphi.sum()))
    return d, recon


# ------------------------------------------------- cắt lớp & phân rã dải (§4.8, §4.8b)

def layer_split(cl, C):
    """Lớp thường L^att = min(L,C); lớp nặng L^exc = max(L−C,0)."""
    g = cl.groupby("Số hồ sơ", observed=True).Incurred.sum()
    att = g.clip(upper=C)
    exc = (g - C).clip(lower=0)
    return dict(n=int(len(g)), att=float(att.sum()), exc=float(exc.sum()),
                n_heavy=int((g > C).sum()))


# Dải thấp nhất mở xuống −∞: có hồ sơ tổn thất ròng ÂM (thu đòi, bút toán điều chỉnh).
# Nếu chặn ở 0 thì số tiền đó rơi ra ngoài mọi dải và tổng phân rã không còn khớp ΔLR.
BANDS = [(-np.inf, 5e6, "Đến 5tr"), (5e6, 10e6, "5–10tr"), (10e6, 30e6, "10–30tr"),
         (30e6, 100e6, "30–100tr"), (100e6, np.inf, "Trên 100tr")]


def band_decomp(p0, c0, p1, c1):
    """Phân rã theo dải — tách 'giá thật sự tăng' khỏi 'phân phối dịch chuyển' (§4.8b).

        Δ_tần suất(k) = (f¹ₖ − f⁰ₖ)·s⁰ₖ / a⁰
        Δ_giá(k)      =  f¹ₖ·(s¹ₖ − s⁰ₖ) / a⁰
        Δ_phí(k)      =  f¹ₖ· s¹ₖ ·(1/a¹ − 1/a⁰)
        Σₖ tổng ba hiệu ứng = LR¹ − LR⁰
    """
    E0, E1 = float(p0.Exposure.sum()), float(p1.Exposure.sum())
    a0, a1 = float(p0.Earned.sum()) / E0, float(p1.Earned.sum()) / E1
    g0 = c0.groupby("Số hồ sơ", observed=True).Incurred.sum()
    g1 = c1.groupby("Số hồ sơ", observed=True).Incurred.sum()
    out = []
    for lo, hi, lab in BANDS:
        m0 = g0[(g0 >= lo) & (g0 < hi)]
        m1 = g1[(g1 >= lo) & (g1 < hi)]
        n0, n1 = len(m0), len(m1)
        f0, f1 = n0 / E0, n1 / E1
        s0 = float(m0.sum()) / n0 if n0 else 0.0
        s1 = float(m1.sum()) / n1 if n1 else 0.0
        out.append(dict(dai=lab, n0=n0, n1=n1, s0=s0, s1=s1, f0=f0, f1=f1,
                        inc0=float(m0.sum()), inc1=float(m1.sum()),
                        d_ts=(f1 - f0) * s0 / a0,
                        d_gia=f1 * (s1 - s0) / a0,
                        d_phi=f1 * s1 * (1 / a1 - 1 / a0)))
    return out


# --------------------------------------------------- chuẩn hoá A/E + kiểm định (§4.6–4.7)

def ae_test(cl_all, cl_seg, cells, C=None):
    """A/E chuẩn hoá gián tiếp + kiểm định z (Workbook §4.6, §4.7).

    cells: danh sách chiều tạo ô chuẩn hoá — phải là chiều mà đối tượng đánh giá
    KHÔNG kiểm soát được. Chiều nằm trong bộ ô sẽ cho A/E ≈ 1 theo cấu tạo.
    Đo trên lớp thường nếu truyền ngưỡng C.
    """
    def prep(d):
        g = d.groupby(["Số hồ sơ"] + cells, observed=True).Incurred.sum().reset_index()
        if C is not None:
            g["Incurred"] = g["Incurred"].clip(upper=C)
        return g
    A, S = prep(cl_all), prep(cl_seg)
    mu = A.groupby(cells, observed=True).Incurred.agg(["mean", "var", "count"])
    mu["var"] = mu["var"].fillna(0.0)
    key = S[cells[0]] if len(cells) == 1 else list(zip(*[S[c] for c in cells]))
    idx = pd.Index(key)
    exp = mu["mean"].reindex(idx).to_numpy()
    var = mu["var"].reindex(idx).to_numpy()
    ok = ~np.isnan(exp)
    E = float(np.nansum(exp[ok]))
    A_ = float(S.Incurred.to_numpy()[ok].sum())
    se = float(np.sqrt(np.nansum(var[ok]))) / E if E > 0 else 0.0
    ae = A_ / E if E > 0 else 0.0
    z = (ae - 1) / se if se > 0 else 0.0
    return dict(thuc=A_, kyvong=E, ae=ae, se=se, z=z, n=int(ok.sum()),
                phu=float(ok.mean()) if len(ok) else 0.0)


def verdict(z):
    """Ba phán quyết chuẩn của §4.7."""
    if z >= 2:
        return "xacnhan"
    if z >= 1:
        return "theodoi"
    return "chuadu"
