// Dựng câu DAX. Thuần chuỗi, không đăng nhập, không mạng — nhờ vậy đối chiếu được từng ký tự với bản
// Python (server.py, vùng _live_*) bằng script chạy ngoài trình duyệt. Đổi một dấu nháy là Power BI
// báo cột không tồn tại.
import { DIM_MAP, STORM_EXCLUDED } from "./model.js";

export const RAW_MEASURES =
  '"Exposure", [Đếm số hợp đồng], "Earned", [Phí thực hưởng], "Incurred", [CPBT gốc năm nay], ' +
  '"Claims", DISTINCTCOUNT(\'Bồi thường CGQ\'[Số hồ sơ]) + DISTINCTCOUNT(\'Bồi thường DGQ\'[Số hồ sơ])';

// Escape theo đúng quy tắc DAX: nhân đôi dấu nháy kép bên trong.
export const daxStr = (v) => '"' + String(v).replace(/"/g, '""') + '"';

export function col(dimId) {
  const [t, c] = DIM_MAP[dimId];
  return { ref: `'${t}'[${c}]`, key: `${t}[${c}]` };
}

export function dateFilter(s, e) {
  const [sy, sm, sd] = s.split("-").map(Number);
  const [ey, em, ed] = e.split("-").map(Number);
  return `'Năm tài chính'[Date] >= DATE(${sy},${sm},${sd}) && 'Năm tài chính'[Date] <= DATE(${ey},${em},${ed})`;
}

export const VCX_LOOKUP_QUERY =
  'EVALUATE SELECTCOLUMNS(\'Mã nghiệp vụ\', "Mã nghiệp vụ", \'Mã nghiệp vụ\'[Mã nghiệp vụ], ' +
  '"Nhóm nghiệp vụ", \'Mã nghiệp vụ\'[Nhóm nghiệp vụ])';

// Bộ lọc bắt buộc cho MỌI truy vấn, dựng lại đúng filter context của báo cáo Power BI. Measure trong
// model KHÔNG tự lọc những điều kiện này: trên báo cáo chúng nằm ở filter cấp trang, thứ không đi kèm
// khi gọi measure qua executeQueries. Thiếu bước này Exposure lệch tới 3,7 lần.
export function vcxFilters(vcxCodes) {
  const vcxSet = "{" + vcxCodes.map(daxStr).join(", ") + "}";
  const stormSet = "{" + STORM_EXCLUDED.map(daxStr).join(", ") + "}";
  return [
    `'DT kế toán'[Mã NV] IN ${vcxSet}`,
    `'DT kế toán'[Nguồn dữ liệu] = "DBV"`,
    `'Bồi thường CGQ'[Mã NV] IN ${vcxSet}`,
    `'Bồi thường DGQ'[Mã NV] IN ${vcxSet}`,
    `NOT('Sự kiện bão'[Sự kiện bão] IN ${stormSet})`,
  ];
}

export function baseFilters(s, e, vcx, path) {
  const f = [dateFilter(s, e), ...vcx];
  if (path?.length) f.push(...path.map((step) => `${col(step.dim).ref} = ${daxStr(step.val)}`));
  return f;
}

const joinFilters = (f) => f.join(",\n    ");

export function portfolioQuery(s, e, vcx, path) {
  return `EVALUATE\nCALCULATETABLE(\n    ROW(${RAW_MEASURES}),\n    ${joinFilters(baseFilters(s, e, vcx, path))}\n)`;
}

export function gridQuery(rowDim, colDim, s, e, vcx) {
  const r = col(rowDim), c = col(colDim);
  return `EVALUATE\nCALCULATETABLE(\n    SUMMARIZECOLUMNS(\n        ${r.ref}, ${c.ref},\n        ${RAW_MEASURES}\n    ),\n    ${joinFilters(baseFilters(s, e, vcx))}\n)`;
}

export function splitQuery(dimId, s, e, vcx, path) {
  const d = col(dimId);
  return `EVALUATE\nCALCULATETABLE(\n    SUMMARIZECOLUMNS(\n        ${d.ref},\n        ${RAW_MEASURES}\n    ),\n    ${joinFilters(baseFilters(s, e, vcx, path))}\n)`;
}
