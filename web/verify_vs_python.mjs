// Đối chiếu bản web (JavaScript) với bản Python đang chạy đúng: cùng khoảng ngày, cùng chiều, so
// từng nhóm. Chạy:  node verify_vs_python.mjs <access_token> [từ-ngày] [đến-ngày]
// Access token lấy bằng: py -3.13 dump_token.py  (ở thư mục app/)
import { readFileSync } from "node:fs";
import { gridQuery, portfolioQuery, splitQuery, vcxFilters, VCX_LOOKUP_QUERY, col } from "./js/dax.js";
import { rowToMetrics, topCategories, bridge, candidatesFromRaw, orderDimValues, shiftPeriod } from "./js/metrics.js";

const [token, S = "2026-01-01", E = "2026-07-31"] = process.argv.slice(2);
if (!token) {
  console.error("Thiếu access token. Chạy: node verify_vs_python.mjs <token>");
  process.exit(1);
}

const CFG = JSON.parse(
  readFileSync(new URL("./config.js", import.meta.url), "utf8").replace(/^[\s\S]*?=\s*/, "").replace(/;\s*$/, "")
    .replace(/(\w+):/g, '"$1":').replace(/,(\s*})/g, "$1")
);

async function runDax(query) {
  const res = await fetch(
    `https://api.powerbi.com/v1.0/myorg/groups/${CFG.workspaceId}/datasets/${CFG.datasetId}/executeQueries`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ queries: [{ query }], serializerSettings: { includeNulls: true } }),
    }
  );
  const data = await res.json();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${JSON.stringify(data).slice(0, 400)}`);
  const err = data?.results?.[0]?.error;
  if (err) throw new Error("DAX: " + JSON.stringify(err));
  return data.results[0].tables[0].rows;
}

const out = { period: [S, E] };

const lookup = await runDax(VCX_LOOKUP_QUERY);
const codes = [...new Set(
  lookup.filter((r) => String(r["[Nhóm nghiệp vụ]"]) === "5.3. BH VCX ô tô").map((r) => String(r["[Mã nghiệp vụ]"]))
)].sort();
const vcx = vcxFilters(codes);
out.vcxCodes = codes;

// toàn danh mục
const pfRows = await runDax(portfolioQuery(S, E, vcx));
const portfolio = rowToMetrics(pfRows[0] || {});
portfolio.ae = 1;
out.portfolio = portfolio;

// lưới kênh x dải giá trị
const ROW_DIM = "kenh", COL_DIM = "daigiatri";
const r = col(ROW_DIM), c = col(COL_DIM);
const gridRows = await runDax(gridQuery(ROW_DIM, COL_DIM, S, E, vcx));
const entries = gridRows
  .filter((x) => x[r.key] !== null && x[c.key] !== null)
  .map((x) => ({ r: String(x[r.key]), c: String(x[c.key]), m: rowToMetrics(x) }));
const top = topCategories(entries, 15);
out.rows = orderDimValues(top.rows);
out.cols = orderDimValues(top.cols);
out.coverage = top.coverage;
out.cells = {};
for (const { r: rv, c: cv, m } of entries) {
  if (out.rows.includes(rv) && out.cols.includes(cv)) out.cells[`${rv}|${cv}`] = { lr: m.lr, freq: m.freq, sev: m.sev, exposure: m.exposure, claims: m.claims };
}

// một đoạn cụ thể + bridge + driver của một chiều
const path = [{ dim: ROW_DIM, val: out.rows[0] }, { dim: COL_DIM, val: out.cols[0] }];
out.path = path;
const segRows = await runDax(portfolioQuery(S, E, vcx, path));
const seg = rowToMetrics(segRows[0] || {});
const expInc = portfolio.freq * portfolio.sev * seg.exposure;
seg.ae = expInc ? seg.incurred / expInc : 0;
out.segment = seg;
out.bridgeVsPortfolio = bridge(portfolio, seg);

const DRIVER_DIM = "hangxe";
const splitRows = await runDax(splitQuery(DRIVER_DIM, S, E, vcx, path));
const d = col(DRIVER_DIM);
const raw = splitRows
  .filter((x) => x[d.key] !== null && x[d.key] !== undefined)
  .map((x) => [String(x[d.key]), rowToMetrics(x)]);
const drivers = candidatesFromRaw(DRIVER_DIM, raw, seg, "lr");
drivers.sort((a, b) => Math.abs(b.impact) - Math.abs(a.impact));
out.drivers = drivers.slice(0, 10);

out.shiftYoY = shiftPeriod(S, E, "yoy");
out.shiftMoM = shiftPeriod(S, E, "mom");

console.log(JSON.stringify(out, null, 1));
