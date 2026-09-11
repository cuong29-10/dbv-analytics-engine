"""Đối chiếu số của nguồn "Kết nối tính toán trực tiếp với Microsoft Fabric" với báo cáo Power BI.

Chạy: py -3.13 verify_live_vs_report.py [YYYY-MM-DD YYYY-MM-DD]

Kiểm hai việc, cả hai đều phải đạt thì mới coi là khớp báo cáo:

  1. Bộ lọc: bộ lọc app đang dùng (_live_base_filters) phải cho ra cùng con số với bộ lọc đọc thẳng
     từ file .pbix của báo cáo (nghiệp vụ VCX ô tô, Nguồn dữ liệu DBV, loại sự kiện thiên tai).
  2. Cách tính: LR/Frequency/Severity/Phí bình quân mà app tự tính từ Exposure/Earned/Incurred/Claims
     phải khớp measure gốc của báo cáo, trên TỪNG NHÓM của từng chiều, chứ không chỉ ở mức tổng.

Ngưỡng nghiệm thu: chỉ số phần trăm lệch dưới 0,5 điểm; chỉ số tiền lệch dưới 0,5%.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import server as S          # noqa: E402
import fabric_extract as F  # noqa: E402

NGUONG_DIEM = 0.5   # điểm phần trăm, cho LR và Frequency
NGUONG_PCT = 0.5    # phần trăm, cho Severity và Phí bình quân

# Bộ lọc đọc thẳng từ Report/definition/report.json + page.json của "Báo cáo quản trị BH XCG".
REPORT_FILTERS = [
    '\'DT kế toán\'[Nguồn dữ liệu] = "DBV"',
    'NOT(\'Sự kiện bão\'[Sự kiện bão] IN {"Lụt Nam Trung Bộ", "Lụt Huế - Đà Nẵng"})',
    '\'Mã nghiệp vụ\'[Nhóm nghiệp vụ] = "5.3. BH VCX ô tô"',
]

MEASURES = (
    '"LR", [TLBT gốc theo thời gian], "Freq", [Tần suất tổn thất], '
    '"Sev", [Mức độ tổn thất], "AvgPrem", [Phí thuần trung bình], '
    '"Exposure", [Đếm số hợp đồng], "Earned", [Phí thực hưởng], '
    '"Incurred", [CPBT gốc năm nay], '
    '"Claims", DISTINCTCOUNT(\'Bồi thường CGQ\'[Số hồ sơ]) + DISTINCTCOUNT(\'Bồi thường DGQ\'[Số hồ sơ])'
)


def _num(row, key):
    v = row.get(key)
    return float(v) if v is not None else 0.0


def tong(filters):
    dax = f"EVALUATE CALCULATETABLE(ROW({MEASURES}),\n    " + ",\n    ".join(filters) + "\n)"
    return F.run_dax(dax)[0]


def kiem_bo_loc(s, e):
    """So bộ lọc app với bộ lọc đọc từ file báo cáo. Cùng kỳ, chạy liền nhau để loại sai lệch do refresh."""
    app = tong(S._live_base_filters(s, e))
    rep = tong([S._dax_date_filter(s, e)] + REPORT_FILTERS)
    print("1) BỘ LỌC: app so với báo cáo")
    dat = True
    for k in ("Exposure", "Earned", "Incurred", "Claims"):
        a, b = _num(app, f"[{k}]"), _num(rep, f"[{k}]")
        lech = (a - b) / b * 100 if b else 0.0
        ok = abs(lech) < NGUONG_PCT
        dat = dat and ok
        print(f"   {k:9s} app={a:20,.1f}  báo cáo={b:20,.1f}  lệch={lech:+8.4f}%  {'đạt' if ok else 'KHÔNG ĐẠT'}")
    return dat


def kiem_cach_tinh(s, e, dims):
    """So chỉ số app tự tính với measure gốc, trên từng nhóm của từng chiều."""
    filt = ",\n    ".join(S._live_base_filters(s, e))
    print()
    print("2) CÁCH TÍNH: app tự tính so với measure gốc, trên từng nhóm")
    print(f"   {'Chiều':14s}{'nhóm':>6s}{'LR':>12s}{'Frequency':>12s}{'Severity':>12s}{'Phí BQ':>12s}")
    dat = True
    for dim_id in dims:
        tc = S.LIVE_DIM_MAP.get(dim_id)
        if not tc:
            continue
        t, c = tc
        dax = (f"EVALUATE CALCULATETABLE(SUMMARIZECOLUMNS('{t}'[{c}], {MEASURES}),\n    {filt}\n)")
        try:
            rows = F.run_dax(dax)
        except Exception as ex:  # noqa: BLE001
            print(f"   {dim_id:14s} LỖI: {str(ex)[:60]}")
            dat = False
            continue
        m_lr = m_fq = m_sv = m_ap = 0.0
        n = 0
        for r in rows:
            ex_, ea = _num(r, "[Exposure]"), _num(r, "[Earned]")
            inc, cl = _num(r, "[Incurred]"), _num(r, "[Claims]")
            if ex_ <= 0 or ea <= 0 or cl <= 0:
                continue
            n += 1
            m_lr = max(m_lr, abs(inc / ea - _num(r, "[LR]")) * 100)
            m_fq = max(m_fq, abs(cl / ex_ - _num(r, "[Freq]")) * 100)
            sv, ap = _num(r, "[Sev]"), _num(r, "[AvgPrem]")
            if sv:
                m_sv = max(m_sv, abs(inc / cl - sv) / sv * 100)
            if ap:
                m_ap = max(m_ap, abs(ea / ex_ - ap) / ap * 100)
        ok = m_lr < NGUONG_DIEM and m_fq < NGUONG_DIEM and m_sv < NGUONG_PCT and m_ap < NGUONG_PCT
        dat = dat and ok
        print(f"   {dim_id:14s}{n:6d}{m_lr:11.6f}đ{m_fq:11.6f}đ{m_sv:11.6f}%{m_ap:11.6f}%"
              f"  {'' if ok else '  KHÔNG ĐẠT'}")
    return dat


def main():
    s = sys.argv[1] if len(sys.argv) > 2 else "2026-01-01"
    e = sys.argv[2] if len(sys.argv) > 2 else "2026-07-31"
    if not F.get_cached_token():
        print("Chưa đăng nhập Fabric trên máy này. Mở app, vào thẻ kéo dữ liệu Fabric và đăng nhập trước.")
        return 2
    print(f"Kỳ đối chiếu: {s} → {e}")
    print(f"Ngưỡng: {NGUONG_DIEM} điểm cho LR/Frequency, {NGUONG_PCT}% cho Severity/Phí BQ")
    print()
    a = kiem_bo_loc(s, e)
    dims = [d for d, v in S.LIVE_DIM_MAP.items() if v]
    b = kiem_cach_tinh(s, e, dims)
    print()
    print("KẾT LUẬN:", "ĐẠT, số khớp báo cáo Power BI" if (a and b) else "KHÔNG ĐẠT, xem dòng đánh dấu ở trên")
    return 0 if (a and b) else 1


if __name__ == "__main__":
    sys.exit(main())
