// Toán nghiệp vụ. Cùng công thức với _row_to_metrics / top_categories / add_ae / engine.bridge của
// bản Python, đã đối chiếu khớp 0,000000 với measure của báo cáo Power BI trên 27 chiều.
import { METRIC_BASE, DIM_LABELS, MIN_CLAIMS_FOR_DRIVER } from "./model.js";

// Bốn số thô từ Fabric -> đủ bộ chỉ số. Tự chia thay vì đọc measure LR/Freq/Sev có sẵn, để cách xử lý
// mẫu số 0 giống hệt đường kéo-toàn-bộ.
export function rowToMetrics(r) {
  const exposure = Number(r["[Exposure]"] || 0);
  const earned = Number(r["[Earned]"] || 0);
  const incurred = Number(r["[Incurred]"] || 0);
  const claims = Number(r["[Claims]"] || 0);
  return {
    exposure, earned, incurred, claims,
    freq: exposure ? claims / exposure : 0,
    sev: claims ? incurred / claims : 0,
    pp: exposure ? incurred / exposure : 0,
    avgprem: exposure ? earned / exposure : 0,
    lr: earned ? incurred / earned : 0,
  };
}

// A/E đơn giản hoá: so Incurred thật với kỳ vọng = freq×sev toàn danh mục áp lên exposure của ô.
export function addAE(entries, portfolio) {
  const pf = portfolio.freq || 0, ps = portfolio.sev || 0;
  for (const { m } of entries) {
    const expInc = pf * ps * m.exposure;
    m.ae = expInc ? m.incurred / expInc : 0;
  }
}

// Xếp theo doanh thu (phí thực hưởng) lớn nhất trước, kèm coverage để không giấu phần bị cắt.
export function topCategories(entries, n) {
  const revRow = new Map(), revCol = new Map();
  for (const { r, c, m } of entries) {
    revRow.set(r, (revRow.get(r) || 0) + m.earned);
    revCol.set(c, (revCol.get(c) || 0) + m.earned);
  }
  const byRev = (map) => [...map.keys()].sort((a, b) => map.get(b) - map.get(a));
  const allRows = byRev(revRow), allCols = byRev(revCol);
  const rows = n ? allRows.slice(0, n) : allRows;
  const cols = n ? allCols.slice(0, n) : allCols;
  const sum = (map, keys) => keys.reduce((t, k) => t + (map.get(k) || 0), 0);
  const totRow = sum(revRow, allRows) || 1, totCol = sum(revCol, allCols) || 1;
  return {
    rows, cols,
    coverage: {
      rowsShown: rows.length, rowsTotal: allRows.length, rowsCoverage: sum(revRow, rows) / totRow,
      colsShown: cols.length, colsTotal: allCols.length, colsCoverage: sum(revCol, cols) / totCol,
    },
  };
}

// Bridge — thay biến theo thứ tự cố định F -> S -> AP.
export function bridge(m0, m1) {
  if (!m0?.avgprem || !m1?.avgprem) return null;
  const lr0 = (m0.freq * m0.sev) / m0.avgprem;
  const lrF = (m1.freq * m0.sev) / m0.avgprem;
  const lrFS = (m1.freq * m1.sev) / m0.avgprem;
  const lr1 = (m1.freq * m1.sev) / m1.avgprem;
  return { lr0, dF: lrF - lr0, dS: lrFS - lrF, dAP: lr1 - lrFS, lr1 };
}

// impact = tỷ trọng của nhóm trên mẫu số của chỉ số × chênh lệch giá trị so với cả đoạn.
// Dấu dương nghĩa là nhóm kéo chỉ số đang chọn cao hơn mức của đoạn, chưa nói tốt hay xấu.
export function candidatesFromRaw(dimId, rawCandidates, segMetrics, metric, minClaims = MIN_CLAIMS_FOR_DRIVER) {
  const label = DIM_LABELS[dimId] || dimId;
  const baseKey = METRIC_BASE[metric] || "incurred";
  const segBaseTotal = segMetrics[baseKey] || 1;
  const segVal = segMetrics[metric] || 0;
  const segPf = segMetrics.freq || 0, segPs = segMetrics.sev || 0;
  const out = [];
  for (const [cat, m] of rawCandidates) {
    if (m.claims < minClaims) continue;
    if (metric === "ae") {
      const expInc = segPf * segPs * m.exposure;
      m.ae = expInc ? m.incurred / expInc : 0;
    }
    const mv = m[metric];
    if (mv === undefined || mv === null) continue;
    const shareBase = m[baseKey] / segBaseTotal;
    out.push({
      dim: dimId, dimLabel: label, category: cat, metricValue: mv,
      shareBase, claims: m.claims, exposure: m.exposure,
      impact: shareBase * (mv - segVal),
    });
  }
  return out;
}

// Kỳ so sánh: yoy lùi đúng một năm, mom lùi đúng độ dài kỳ đang xem.
export function shiftPeriod(s, e, mode) {
  const d = (x) => new Date(x + "T00:00:00Z");
  const iso = (x) => x.toISOString().slice(0, 10);
  const ds = d(s), de = d(e);
  if (mode === "yoy") {
    const back = (x) => {
      const y = new Date(x);
      y.setUTCFullYear(y.getUTCFullYear() - 1);
      return y;
    };
    return [iso(back(ds)), iso(back(de))];
  }
  if (mode === "mom") {
    const span = Math.round((de - ds) / 86400000) + 1;
    const prevEnd = new Date(ds.getTime() - 86400000);
    const prevStart = new Date(prevEnd.getTime() - (span - 1) * 86400000);
    return [iso(prevStart), iso(prevEnd)];
  }
  return [null, null];
}

// Thứ tự hiển thị: nhãn có đánh số nghiệp vụ ("1. Dưới 400 tr", "II. XE CHỞ HÀNG") thì sắp theo số đó
// thay vì alphabet, nhưng chỉ khi tuyệt đại đa số giá trị khớp mẫu. Nhãn "không xác định" luôn xếp cuối.
const ROMAN = { I: 1, V: 5, X: 10, L: 50, C: 100, D: 500, M: 1000 };
const NUM_PREFIX = /^\s*(\d+(?:\.\d+)*)\s*\.\s/;
const ROMAN_PREFIX = /^\s*([IVXLCDM]+)\s*\.\s/i;
const UNKNOWN_LABELS = new Set(["không xác định", "chưa phân loại", "không phân loại"]);

function romanToInt(s) {
  let total = 0, prev = 0;
  for (const ch of [...s.toUpperCase()].reverse()) {
    const v = ROMAN[ch];
    if (v === undefined) return null;
    total += v < prev ? -v : v;
    prev = Math.max(prev, v);
  }
  return total;
}

function ordinalKey(value) {
  const s = String(value);
  const m = NUM_PREFIX.exec(s);
  if (m) return m[1].split(".").map(Number);
  const r = ROMAN_PREFIX.exec(s);
  if (r) {
    const n = romanToInt(r[1]);
    if (n !== null) return [n];
  }
  return null;
}

function cmpKey(a, b) {
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    const x = a[i] ?? -Infinity, y = b[i] ?? -Infinity;
    if (x !== y) return x - y;
  }
  return 0;
}

export function orderDimValues(values) {
  const known = values.filter((v) => !UNKNOWN_LABELS.has(String(v).trim().toLowerCase()));
  const unknown = values.filter((v) => UNKNOWN_LABELS.has(String(v).trim().toLowerCase()));
  if (!known.length) return values;
  const keys = known.map(ordinalKey);
  if (keys.filter(Boolean).length / known.length < 0.8) return values;
  const ordered = known
    .map((v, i) => ({ v, k: keys[i] }))
    .sort((a, b) => (a.k && b.k ? cmpKey(a.k, b.k) : a.k ? -1 : b.k ? 1 : String(a.v).localeCompare(String(b.v))))
    .map((x) => x.v);
  return [...ordered, ...unknown];
}
