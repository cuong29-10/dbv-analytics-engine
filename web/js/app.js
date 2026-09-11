// Nối giao diện với lớp truy vấn. Mọi con số hiển thị đều do metrics.js tính từ bốn số thô Fabric
// trả về, không có giá trị nào dựng sẵn.
import { DIM_LABELS, DIM_IDS, METRICS, MIN_CLAIMS_FOR_DRIVER, CLAIMS_ONLY_DIMS } from "./model.js";
import { initAuth, login, logout, lastRefresh } from "./fabric.js";
import * as Q from "./query.js";
import {
  addAE, topCategories, bridge, candidatesFromRaw, shiftPeriod, orderDimValues,
} from "./metrics.js";

const $ = (id) => document.getElementById(id);
const esc = (v) => String(v).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const FMT = {
  lr: (v) => (v * 100).toFixed(1) + "%",
  freq: (v) => v.toFixed(3),
  sev: (v) => (v / 1e6).toFixed(2) + "tr",
  pp: (v) => (v / 1e6).toFixed(2) + "tr",
  avgprem: (v) => (v / 1e6).toFixed(2) + "tr",
  ae: (v) => v.toFixed(2),
};
const HIGHER_IS_WORSE = { lr: true, freq: true, sev: true, pp: true, avgprem: false, ae: true };
const num = (v, d = 0) => v.toLocaleString("vi-VN", { minimumFractionDigits: d, maximumFractionDigits: d });
const money = (v) => (Math.abs(v) >= 1e9 ? (v / 1e9).toFixed(1) + " tỷ" : (v / 1e6).toFixed(1) + " tr");

const state = { res: null, params: null };

// ---------------------------------------------------------------- khởi động
async function boot() {
  const dimOpts = DIM_IDS.map((d) => `<option value="${d}">${esc(DIM_LABELS[d])}</option>`).join("");
  $("fRow").innerHTML = dimOpts;
  $("fCol").innerHTML = dimOpts;
  $("fRow").value = "kenh";
  $("fCol").value = "daigiatri";
  $("fMetric").innerHTML = METRICS.map((m) => `<option value="${m.id}">${esc(m.name)}</option>`).join("");

  const today = new Date();
  $("fTo").value = today.toISOString().slice(0, 10);
  $("fFrom").value = `${today.getUTCFullYear()}-01-01`;

  $("btnLogin").onclick = () => login();
  $("btnRun").onclick = () => run();
  $("btnCloseDrill").onclick = () => $("drillOverlay").classList.remove("open");
  $("drillOverlay").onclick = (e) => { if (e.target === $("drillOverlay")) $("drillOverlay").classList.remove("open"); };
  $("heatGrid").onclick = (e) => {
    const el = e.target.closest(".cell.clickable");
    if (el) openDrill(el.dataset.row, el.dataset.col);
  };

  let account;
  try {
    account = await initAuth();
  } catch (e) {
    $("gate").style.display = "";
    $("gateErr").style.display = "";
    $("gateErr").textContent = "Không khởi tạo được đăng nhập: " + e.message;
    return;
  }
  if (!account) {
    $("gate").style.display = "";
    $("chipFresh").textContent = "Chưa đăng nhập";
    return;
  }
  $("app").style.display = "";
  const chip = $("chipAccount");
  chip.style.display = "";
  chip.classList.add("on");
  chip.innerHTML = `${esc(account.username)}<a href="#" id="btnLogout">Đăng xuất</a>`;
  $("btnLogout").onclick = (e) => { e.preventDefault(); logout(); };
  showFreshness();
}

async function showFreshness() {
  try {
    const iso = await lastRefresh();
    if (!iso) throw new Error();
    const d = new Date(iso);
    const p = (n) => String(n).padStart(2, "0");
    $("chipFresh").textContent =
      `Số liệu tới ${p(d.getHours())}:${p(d.getMinutes())} ngày ${p(d.getDate())}/${p(d.getMonth() + 1)}/${d.getFullYear()}`;
    $("chipFresh").classList.add("on");
  } catch {
    $("chipFresh").textContent = "Chưa lấy được mốc làm mới";
  }
}

// ---------------------------------------------------------------- chạy phân tích
function busy(on, msg) {
  $("spinMain").classList.toggle("on", on);
  $("statusMain").textContent = msg || "";
  $("btnRun").disabled = on;
}

function fail(msg) {
  $("errMain").style.display = "";
  $("errMain").textContent = msg;
}

async function run() {
  const p = {
    s: $("fFrom").value, e: $("fTo").value,
    rowDim: $("fRow").value, colDim: $("fCol").value,
    metric: $("fMetric").value, mode: $("fMode").value,
    topN: Number($("fTop").value) || null,
  };
  if (!p.s || !p.e) return fail("Chọn đủ khoảng ngày trước.");
  if (p.s > p.e) return fail("Từ ngày phải trước Đến ngày.");
  if (p.rowDim === p.colDim) return fail("Chọn hai chiều khác nhau cho hàng và cột.");
  $("errMain").style.display = "none";

  busy(true, "Đang truy vấn Microsoft Fabric…");
  const t0 = performance.now();
  try {
    const [gridRes, pfRes] = await Promise.all([
      Q.grid(p.rowDim, p.colDim, p.s, p.e),
      Q.portfolio(p.s, p.e),
    ]);
    const entries = gridRes.value, portfolio = pfRes.value;
    portfolio.ae = 1;
    if (p.metric === "ae") addAE(entries, portfolio);

    let prevEntries = null, prevIndex = null;
    if (p.mode !== "current") {
      const [ps, pe] = shiftPeriod(p.s, p.e, p.mode);
      busy(true, "Đang truy vấn kỳ so sánh…");
      const [pg, pp] = await Promise.all([Q.grid(p.rowDim, p.colDim, ps, pe), Q.portfolio(ps, pe)]);
      prevEntries = pg.value;
      if (p.metric === "ae") addAE(prevEntries, pp.value);
      prevIndex = Q.indexCells(prevEntries);
      p.prevPeriod = [ps, pe];
    }

    const { rows, cols, coverage } = topCategories(entries, p.topN);
    state.res = {
      index: Q.indexCells(entries),
      prevIndex,
      rows: orderDimValues(rows),
      cols: orderDimValues(cols),
      portfolio, coverage,
    };
    state.params = p;
    render(performance.now() - t0);
  } catch (e) {
    fail(e.message);
  } finally {
    busy(false, "");
  }
}

function render(ms) {
  const { portfolio, coverage, rows, cols } = state.res;
  const p = state.params;
  $("emptyCard").style.display = "none";
  $("resultCard").style.display = "";

  $("kpis").innerHTML = [
    ["Tỷ lệ bồi thường", FMT.lr(portfolio.lr)],
    ["Phí thực hưởng", money(portfolio.earned)],
    ["Bồi thường phát sinh", money(portfolio.incurred)],
    ["Exposure (xe-năm)", num(portfolio.exposure, 0)],
    ["Số hồ sơ", num(portfolio.claims)],
  ].map(([k, v]) => `<div class="kpi"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");

  const metricName = METRICS.find((m) => m.id === p.metric).name;
  $("heatTitle").textContent = `${metricName}: ${DIM_LABELS[p.rowDim]} × ${DIM_LABELS[p.colDim]}`;
  $("heatSub").textContent =
    (p.mode === "current" ? `Kỳ ${p.s} đến ${p.e}` : `Chênh lệch so với kỳ ${p.prevPeriod[0]} đến ${p.prevPeriod[1]}`) +
    ` · truy vấn xong sau ${(ms / 1000).toFixed(1)} giây · bấm vào ô bất kỳ để đào sâu`;

  drawHeat();
  let note =
    `Xếp theo doanh thu (phí thực hưởng) lớn nhất trước. Đang hiện ${coverage.rowsShown}/${coverage.rowsTotal} nhóm ` +
    `${esc(DIM_LABELS[p.rowDim])} (phủ ${(coverage.rowsCoverage * 100).toFixed(1)}% doanh thu) và ` +
    `${coverage.colsShown}/${coverage.colsTotal} nhóm ${esc(DIM_LABELS[p.colDim])} ` +
    `(phủ ${(coverage.colsCoverage * 100).toFixed(1)}%).`;
  const claimsOnly = [p.rowDim, p.colDim].filter((d) => CLAIMS_ONLY_DIMS.has(d));
  if (claimsOnly.length && p.metric !== "sev") {
    note += `<br><br><b>Lưu ý:</b> ${claimsOnly.map((d) => esc(DIM_LABELS[d])).join(" và ")} chỉ có bên hồ sơ
      bồi thường, model không chia phí bảo hiểm theo chiều này nên mỗi nhóm nhận nguyên phí của cả đoạn.
      Chỉ nên đọc Severity theo chiều này; tỷ lệ bồi thường và tần suất không so sánh được giữa các nhóm.`;
  }
  $("coverageNote").innerHTML = note;
}

// ---------------------------------------------------------------- heatmap
const LR_BENCHMARK = [[0.6, "bm1"], [0.65, "bm2"], [0.7, "bm3"], [0.75, "bm4"], [0.85, "bm5"]];

function drawHeat() {
  const { index, prevIndex, rows, cols } = state.res;
  const p = state.params;
  const useDelta = p.mode !== "current";
  const useBenchmark = p.metric === "lr" && !useDelta;
  const fmt = FMT[p.metric];

  const valueOf = (r, c) => {
    const m = index.get(Q.cellKey(r, c));
    if (!m) return null;
    if (!useDelta) return m[p.metric];
    const pm = prevIndex?.get(Q.cellKey(r, c));
    return pm ? m[p.metric] - pm[p.metric] : null;
  };

  const vals = [];
  rows.forEach((r) => cols.forEach((c) => {
    const v = valueOf(r, c);
    if (v !== null && !Number.isNaN(v)) vals.push(v);
  }));
  const lo = vals.length ? Math.min(...vals) : 0;
  const hi = vals.length ? Math.max(...vals) : 1;
  const maxAbs = vals.length ? Math.max(...vals.map(Math.abs)) : 0;

  const bucketLR = (v) => (LR_BENCHMARK.find(([th]) => v < th) || [, "bm6"])[1];
  const bucketRel = (v) => {
    if (hi === lo) return "hv3";
    const t = (v - lo) / (hi - lo);
    return t < 0.2 ? "hv1" : t < 0.4 ? "hv2" : t < 0.6 ? "hv3" : t < 0.8 ? "hv4" : "hv5";
  };
  // So kỳ: giảm có thể tốt hoặc xấu tuỳ chỉ số (LR giảm là tốt, Average Premium giảm là xấu), nên
  // tách theo chiều tốt/xấu rồi mới tô đậm nhạt theo độ lớn chênh lệch.
  const bucketDelta = (d) => {
    if (d === 0 || maxAbs === 0) return "dz";
    const favorable = HIGHER_IS_WORSE[p.metric] !== false ? d < 0 : d > 0;
    const t = Math.abs(d) / maxAbs;
    return (favorable ? "dg" : "db") + (t < 0.33 ? 1 : t < 0.66 ? 2 : 3);
  };
  const bucket = useBenchmark ? bucketLR : useDelta ? bucketDelta : bucketRel;

  let html = `<div class="cell corner"></div>` +
    cols.map((c) => `<div class="cell collab" title="${esc(c)}">${esc(c)}</div>`).join("");
  for (const r of rows) {
    html += `<div class="cell rowlab" title="${esc(r)}">${esc(r)}</div>`;
    for (const c of cols) {
      const v = valueOf(r, c);
      if (v === null || Number.isNaN(v)) {
        html += `<div class="cell empty">—</div>`;
        continue;
      }
      const txt = useDelta ? (v > 0 ? "+" : "") + fmt(v) : fmt(v);
      html += `<div class="cell clickable ${bucket(v)}" data-row="${esc(r)}" data-col="${esc(c)}" ` +
        `title="Bấm để đào sâu">${txt}</div>`;
    }
  }
  $("heatGrid").innerHTML = html;
  $("heatGrid").style.gridTemplateColumns = `minmax(140px,1.4fr) repeat(${cols.length},minmax(74px,1fr))`;

  const sw = (cls, label) => `<span><span class="sw ${cls}"></span>${label}</span>`;
  $("legend").innerHTML = useBenchmark
    ? "Ngưỡng cố định: " + [["bm1", "<60%"], ["bm2", "60-65%"], ["bm3", "65-70%"], ["bm4", "70-75%"],
        ["bm5", "75-85%"], ["bm6", ">85%"]].map(([c, l]) => sw(c, l)).join("")
    : useDelta
      ? "So kỳ trước: " + [["dg3", "cải thiện"], ["dz", "gần như không đổi"], ["db3", "xấu đi"]].map(([c, l]) => sw(c, l)).join("")
      : "Thang theo khoảng giá trị đang hiện: " + [["hv1", "thấp"], ["hv3", "giữa"], ["hv5", "cao"]].map(([c, l]) => sw(c, l)).join("");
}

// ---------------------------------------------------------------- đào sâu
async function openDrill(rowVal, colVal) {
  const p = state.params;
  const path = [{ dim: p.rowDim, val: rowVal }, { dim: p.colDim, val: colVal }];
  $("drillOverlay").classList.add("open");
  $("drillTitle").textContent = `${rowVal} × ${colVal}`;
  $("drillSub").textContent = `${DIM_LABELS[p.rowDim]} × ${DIM_LABELS[p.colDim]} · kỳ ${p.s} đến ${p.e}`;
  $("drillBody").innerHTML = `<div class="status"><span class="spinner on"></span>
    <span id="drillProgress">Đang lấy số liệu của đoạn…</span></div>`;

  try {
    const portfolio = state.res.portfolio;
    const seg = (await Q.segment(path, p.s, p.e)).value;
    const expInc = (portfolio.freq || 0) * (portfolio.sev || 0) * seg.exposure;
    seg.ae = expInc ? seg.incurred / expInc : 0;

    let prevBlock = null;
    if (p.mode !== "current") {
      const [ps, pe] = p.prevPeriod;
      const pseg = (await Q.segment(path, ps, pe)).value;
      const ppf = (await Q.portfolio(ps, pe)).value;
      const pExpInc = (ppf.freq || 0) * (ppf.sev || 0) * pseg.exposure;
      pseg.ae = pExpInc ? pseg.incurred / pExpInc : 0;
      prevBlock = { period: [ps, pe], metrics: pseg, bridge: bridge(pseg, seg) };
    }

    renderDrill(seg, portfolio, prevBlock, null);

    const prog = $("drillProgress");
    const scanned = await Q.scanAllDims(path, p.s, p.e, (done, total) => {
      if (prog) prog.textContent = `Đang quét nguyên nhân: ${done}/${total} chiều…`;
    });
    const drivers = [];
    for (const [dimId, raw] of scanned) {
      drivers.push(...candidatesFromRaw(dimId, raw, seg, p.metric, MIN_CLAIMS_FOR_DRIVER));
    }
    drivers.sort((a, b) => Math.abs(b.impact) - Math.abs(a.impact));
    renderDrill(seg, portfolio, prevBlock, drivers.slice(0, 24));
  } catch (e) {
    $("drillBody").innerHTML = `<div class="err">${esc(e.message)}</div>`;
  }
}

function renderDrill(seg, portfolio, prevBlock, drivers) {
  const p = state.params;
  const kpi = (k, v) => `<div class="kpi"><div class="k">${k}</div><div class="v">${v}</div></div>`;
  let html = `<div class="sec"><h3>Chỉ số của đoạn</h3><div class="kpis">
    ${kpi("Tỷ lệ bồi thường", FMT.lr(seg.lr))}
    ${kpi("Frequency", FMT.freq(seg.freq))}
    ${kpi("Severity", FMT.sev(seg.sev))}
    ${kpi("Average Premium", FMT.avgprem(seg.avgprem))}
    ${kpi("Exposure", num(seg.exposure, 0))}
    ${kpi("Số hồ sơ", num(seg.claims))}
  </div></div>`;

  const br = bridge(portfolio, seg);
  if (br) html += bridgeBlock("Phân rã chênh lệch so với toàn danh mục", br);
  if (prevBlock?.bridge) {
    html += bridgeBlock(`Phân rã chênh lệch so với kỳ ${prevBlock.period[0]} đến ${prevBlock.period[1]}`, prevBlock.bridge);
  }

  html += `<div class="sec"><h3>Nguyên nhân theo các chiều còn lại</h3>`;
  if (drivers === null) {
    html += `<div class="status"><span class="spinner on"></span>
      <span id="drillProgress">Đang quét nguyên nhân…</span></div>`;
  } else if (!drivers.length) {
    html += `<div class="sub">Không có nhóm nào đủ ${MIN_CLAIMS_FOR_DRIVER} hồ sơ để kết luận.</div>`;
  } else {
    const metricName = METRICS.find((m) => m.id === p.metric).name;
    html += `<div class="sub" style="margin-bottom:8px">Đóng góp là tỷ trọng của nhóm nhân với chênh lệch
      ${esc(metricName.toLowerCase())} so với cả đoạn. Dấu dương nghĩa là nhóm kéo chỉ số lên cao hơn mức của đoạn.</div>
      <table><thead><tr><th class="l">Chiều</th><th class="l">Nhóm</th><th>Giá trị</th>
      <th>Tỷ trọng</th><th>Số hồ sơ</th><th>Đóng góp</th></tr></thead><tbody>` +
      drivers.map((d) => {
        const small = d.claims < 15 ? ` <span class="tag warn">mẫu nhỏ</span>` : "";
        const cls = d.impact > 0 ? "pos" : "neg";
        const sign = d.impact > 0 ? "+" : "";
        return `<tr><td class="l">${esc(d.dimLabel)}</td><td class="l">${esc(d.category)}${small}</td>
          <td>${FMT[p.metric](d.metricValue)}</td><td>${(d.shareBase * 100).toFixed(1)}%</td>
          <td>${num(d.claims)}</td><td class="${cls}">${sign}${FMT[p.metric](d.impact)}</td></tr>`;
      }).join("") + `</tbody></table>`;
  }
  html += `</div>`;
  $("drillBody").innerHTML = html;
}

function bridgeBlock(title, br) {
  const step = (k, v, signed) => {
    const cls = signed ? (v > 0 ? "pos" : "neg") : "";
    const sign = signed && v > 0 ? "+" : "";
    return `<div class="step"><div class="k">${k}</div><div class="v ${cls}">${sign}${(v * 100).toFixed(1)}%</div></div>`;
  };
  return `<div class="sec"><h3>${esc(title)}</h3>
    <div class="sub" style="margin-bottom:6px">Thay lần lượt Frequency, Severity rồi Average Premium
      để thấy mỗi yếu tố đóng góp bao nhiêu vào chênh lệch tỷ lệ bồi thường.</div>
    <div class="bridge">
      ${step("Điểm xuất phát", br.lr0, false)}
      ${step("Do Frequency", br.dF, true)}
      ${step("Do Severity", br.dS, true)}
      ${step("Do Average Premium", br.dAP, true)}
      ${step("Kết quả", br.lr1, false)}
    </div></div>`;
}

boot();
