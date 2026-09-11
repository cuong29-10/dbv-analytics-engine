"""Chạy bản Python trên đúng tham số của verify_vs_python.mjs rồi so từng con số.

    cd app/web
    py -3.13 verify_vs_python.py <duong_dan_js_out.json>

Khớp tuyệt đối là mục tiêu: hai bản gọi cùng measure, cùng bộ lọc, cùng công thức — lệch nghĩa là
một bên dựng sai câu DAX hoặc sai phép chia, không phải sai số chấp nhận được.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server as S  # noqa: E402
import webauth as WA  # noqa: E402

TOL = 1e-9


def close(a, b):
    if a is None or b is None:
        return a is None and b is None
    scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) / scale < TOL


def main():
    js = json.load(open(sys.argv[1], encoding="utf-8"))
    s, e = js["period"]

    sess, _ = WA.get_or_create(None)
    app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sess["cache"] = open(os.path.join(app_dir, "fabric_token_cache.bin"), encoding="utf-8").read()
    WA.bind(sess)
    S.FABRIC.SESSION_TOKEN.set(WA.access_token(S.FABRIC, sess))

    fails = []

    def check(name, py, jsv):
        ok = close(py, jsv) if isinstance(py, (int, float)) else py == jsv
        if not ok:
            fails.append(f"{name}: python={py!r} js={jsv!r}")
        return ok

    # mã nghiệp vụ VCX
    _nv, vcx = S.FABRIC._vcx_codes()
    check("vcxCodes", sorted(vcx), sorted(js["vcxCodes"]))

    # toàn danh mục
    pf = S._live_portfolio_metrics(s, e)
    for k in ("exposure", "earned", "incurred", "claims", "freq", "sev", "pp", "avgprem", "lr"):
        check(f"portfolio.{k}", pf[k], js["portfolio"][k])

    # lưới + top N + coverage
    cells = S._live_cell_metrics("kenh", "daigiatri", s, e)
    rows, cols, coverage = S.top_categories(cells, 15)
    rows = S.order_dim_values("kenh", rows)
    cols = S.order_dim_values("daigiatri", cols)
    check("rows", rows, js["rows"])
    check("cols", cols, js["cols"])
    for k in coverage:
        check(f"coverage.{k}", coverage[k], js["coverage"][k])

    n_cells = 0
    for (r, c), m in cells.items():
        if r not in rows or c not in cols:
            continue
        key = f"{r}|{c}"
        if key not in js["cells"]:
            fails.append(f"cells[{key}]: thiếu bên js")
            continue
        for k in ("lr", "freq", "sev", "exposure", "claims"):
            check(f"cells[{key}].{k}", m[k], js["cells"][key][k])
        n_cells += 1
    if n_cells != len(js["cells"]):
        fails.append(f"số ô: python={n_cells} js={len(js['cells'])}")

    # một đoạn + bridge
    path = [{"dim": p["dim"], "val": p["val"]} for p in js["path"]]
    seg = S._live_segment_metrics(path, s, e)
    exp_inc = pf["freq"] * pf["sev"] * seg["exposure"]
    seg["ae"] = (seg["incurred"] / exp_inc) if exp_inc else 0.0
    for k in ("exposure", "earned", "incurred", "claims", "freq", "sev", "lr", "ae"):
        check(f"segment.{k}", seg[k], js["segment"][k])

    pf_for_bridge = dict(pf)
    pf_for_bridge["ae"] = 1.0
    br = S.safe_bridge(pf_for_bridge, seg)
    for k in ("lr0", "dF", "dS", "dAP", "lr1"):
        check(f"bridge.{k}", br[k], js["bridgeVsPortfolio"][k])

    # driver của một chiều
    raw, _ = S._live_split(path, "hangxe", s, e)
    drv = S._live_candidates_from_raw("hangxe", raw, seg, "lr", min_claims=S.MIN_CLAIMS_FOR_DRIVER)
    drv.sort(key=lambda c: -abs(c["impact"]))
    drv = drv[:10]
    check("drivers.len", len(drv), len(js["drivers"]))
    for i, (a, b) in enumerate(zip(drv, js["drivers"])):
        check(f"drivers[{i}].category", a["category"], b["category"])
        for k in ("metricValue", "shareBase", "impact", "claims", "exposure"):
            check(f"drivers[{i}].{k}", a[k], b[k])

    # dịch kỳ
    for mode, key in (("yoy", "shiftYoY"), ("mom", "shiftMoM")):
        ps, pe = S.shift_period(s, e, mode)
        check(f"shift.{mode}", [ps.date().isoformat(), pe.date().isoformat()], js[key])

    print(f"Kỳ {s} -> {e}")
    print(f"Đã so: toàn danh mục, {n_cells} ô của lưới Kênh × Giá trị xe, coverage, 1 đoạn đào sâu, "
          f"bridge, {len(drv)} driver, dịch kỳ YoY/MoM.")
    if fails:
        print(f"\nLỆCH {len(fails)} chỗ:")
        for f in fails[:40]:
            print("  -", f)
        sys.exit(1)
    print("\nKhớp tuyệt đối trên mọi giá trị đã so.")


if __name__ == "__main__":
    main()
