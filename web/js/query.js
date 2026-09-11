// Gọi Fabric và gom kết quả. Việc dựng chuỗi DAX nằm ở dax.js; ở đây chỉ có cache, phân trang kết quả
// và chuyển đổi sang bộ chỉ số.
import { DIM_IDS, VCX_GROUP, CLAIMS_ONLY_DIMS } from "./model.js";
import { runDax } from "./fabric.js";
import { rowToMetrics } from "./metrics.js";
import * as D from "./dax.js";

let vcxCache = null;

async function vcx() {
  if (vcxCache) return vcxCache;
  const rows = await runDax(D.VCX_LOOKUP_QUERY);
  const codes = [...new Set(
    rows.filter((r) => String(r["[Nhóm nghiệp vụ]"]) === VCX_GROUP).map((r) => String(r["[Mã nghiệp vụ]"]))
  )].sort();
  if (!codes.length) throw new Error("Không tìm thấy mã nghiệp vụ VCX ô tô trên Fabric.");
  vcxCache = D.vcxFilters(codes);
  return vcxCache;
}

// ---------------------------------------------------------------- cache trong phiên trình duyệt
// Đổi chỉ số hay Top N trên cùng một lưới không phải gọi lại Fabric.
const cache = new Map();

async function cached(key, fn) {
  if (cache.has(key)) return { value: cache.get(key), ms: 0 };
  const t0 = performance.now();
  const value = await fn();
  cache.set(key, value);
  return { value, ms: performance.now() - t0 };
}

const pathKey = (path) => (path || []).map((p) => `${p.dim}=${p.val}`).join("|");

// ---------------------------------------------------------------- truy vấn
export async function portfolio(s, e) {
  return cached(`pf|${s}|${e}`, async () => {
    const rows = await runDax(D.portfolioQuery(s, e, await vcx()));
    return rowToMetrics(rows[0] || {});
  });
}

export async function grid(rowDim, colDim, s, e) {
  return cached(`grid|${rowDim}|${colDim}|${s}|${e}`, async () => {
    const r = D.col(rowDim), c = D.col(colDim);
    const rows = await runDax(D.gridQuery(rowDim, colDim, s, e, await vcx()));
    const entries = [];
    for (const row of rows) {
      const rv = row[r.key], cv = row[c.key];
      if (rv === null || rv === undefined || cv === null || cv === undefined) continue;
      entries.push({ r: String(rv), c: String(cv), m: rowToMetrics(row) });
    }
    return entries;
  });
}

export async function segment(path, s, e) {
  return cached(`seg|${pathKey(path)}|${s}|${e}`, async () => {
    const rows = await runDax(D.portfolioQuery(s, e, await vcx(), path));
    return rowToMetrics(rows[0] || {});
  });
}

// Cắt một đoạn theo một chiều: trả [[giá trị, metrics], ...]
export async function split(path, dimId, s, e) {
  return cached(`split|${dimId}|${pathKey(path)}|${s}|${e}`, async () => {
    const d = D.col(dimId);
    const rows = await runDax(D.splitQuery(dimId, s, e, await vcx(), path));
    return rows
      .filter((r) => r[d.key] !== null && r[d.key] !== undefined)
      .map((r) => [String(r[d.key]), rowToMetrics(r)]);
  });
}

// Quét mọi chiều còn lại để tìm nhóm kéo chỉ số lệch nhiều nhất. Mỗi chiều là một lượt gọi riêng —
// SUMMARIZECOLUMNS nhiều chiều cùng lúc cho ra tích Descartes chứ không phải quét từng chiều.
export async function scanAllDims(path, s, e, onProgress) {
  const used = new Set(path.map((p) => p.dim));
  const dims = DIM_IDS.filter((d) => !used.has(d) && !CLAIMS_ONLY_DIMS.has(d));
  const out = [];
  for (let i = 0; i < dims.length; i++) {
    try {
      const { value } = await split(path, dims[i], s, e);
      out.push([dims[i], value]);
    } catch {
      // Một chiều lỗi không nên làm hỏng cả lượt quét.
    }
    onProgress?.(i + 1, dims.length);
  }
  return out;
}

// Giá trị chiều chứa dấu cách, dấu chấm, dấu gạch — khoá ô phải dùng ký tự không bao giờ có trong dữ
// liệu thật, nếu không hai ô khác nhau có thể trùng khoá.
const SEP = String.fromCharCode(1);
export const cellKey = (r, c) => r + SEP + c;

export function indexCells(entries) {
  const map = new Map();
  for (const x of entries) map.set(cellKey(x.r, x.c), x.m);
  return map;
}
