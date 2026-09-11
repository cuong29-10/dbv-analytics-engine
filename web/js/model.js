// Bản đồ chiều và các hằng số nghiệp vụ. Giữ đúng tên bảng/cột như LIVE_DIM_MAP trong server.py —
// sai một ký tự là Power BI báo lỗi cột không tồn tại.

export const DIM_LABELS = {
  donvi: "Đơn vị", hangxe: "Hãng xe", kenh: "Kênh khai thác", daigiatri: "Giá trị xe",
  mucdich: "Mục đích sử dụng xe", nhomrr: "Nhóm rủi ro địa bàn",
  hieuxe: "Hiệu xe", dongxe: "Dòng xe",
  phankhuc: "Phân khúc", nhienlieu: "Nhiên liệu", sochongoi: "Số chỗ ngồi",
  kieuthanxe: "Kiểu thân xe", phongkd: "Phòng kinh doanh", canbokt: "Cán bộ kinh doanh",
  doitac: "Đối tác", diemban: "Điểm bán", sochoden: "Số chỗ", trongtaikg: "Trọng tải kg (đến)",
  diaban: "Địa bàn", nhomgara: "Phân nhóm SH/GR sửa chữa", garatt: "Gara sửa chữa",
  nhommucdo: "Nhóm mức độ tổn thất",
  loaixe_f0: "Nhóm xe cấp 1 (F0)", loaixe_f1: "Nhóm xe cấp 2 (F1)",
  loaixe_f2: "Nhóm xe cấp 3 (F2)", ftnds: "Phân loại TNDS (FTNDS)",
};

// dimId -> [tên bảng, tên cột] trong model Fabric.
// Hai chiều bị bỏ hẳn vì model không cắt được số:
//   "dongco" — bảng "Động cơ" trên Fabric rỗng, truy vấn luôn lỗi.
//   "thoigiansd" — cột nằm trên chính bảng doanh thu "DT kế toán", không có đường liên kết sang hai
//     bảng bồi thường. Cắt theo nó thì phí được chia đúng nhưng bồi thường giữ nguyên của cả đoạn và
//     bị đếm lại ở từng nhóm: đo thực tế tổng bồi thường của 6 nhóm bằng đúng 6 lần tổng thật, làm tỷ
//     lệ bồi thường vọt lên tới 3.152%. Bản Python hiện cũng còn lỗi này.
export const DIM_MAP = {
  donvi: ["Mã đơn vị", "ĐƠN VỊ"],
  hangxe: ["Phân loại hiệu xe hãng xe", "Hãng xe làm sạch"],
  kenh: ["Kênh khai thác", "Kênh"],
  daigiatri: ["Giá trị xe", "Giá trị xe"],
  mucdich: ["Phân nhóm loại xe", "MDSD"],
  nhomrr: ["Địa bàn hoạt động", "Phân nhóm rủi ro từng địa bàn"],
  hieuxe: ["Phân loại hiệu xe hãng xe", "Hiệu xe"],
  dongxe: ["Phân loại hiệu xe hãng xe", "Dòng xe"],
  phankhuc: ["Phân loại hiệu xe hãng xe", "Phân khúc"],
  nhienlieu: ["Phân loại hiệu xe hãng xe", "Nhiên liệu"],
  sochongoi: ["Phân loại hiệu xe hãng xe", "Số chỗ ngồi"],
  kieuthanxe: ["Phân loại hiệu xe hãng xe", "Kiểu thân xe"],
  phongkd: ["Mã phòng", "Tên phòng"],
  canbokt: ["Mã cán bộ", "Tên cán bộ khai thác"],
  doitac: ["Mã nguồn DV", "Nhóm nguồn khai thác"],
  diemban: ["Mã nguồn DV", "Tên tắt"],
  sochoden: ["Phân nhóm loại xe", "Số chỗ (đến)"],
  trongtaikg: ["Phân nhóm loại xe", "Trọng tải kg (đến)"],
  diaban: ["Địa bàn hoạt động", "Địa bàn (theo mã biển số)"],
  nhomgara: ["Gara sửa chữa", "Phân nhóm SH/GR"],
  garatt: ["Gara sửa chữa", "TENTHUONGGOI_TEN_TAT"],
  // Chiều chỉ có bên Bồi thường: phía Hợp đồng không cắt theo nó được nên mỗi nhóm nhận nguyên
  // Exposure của cả đoạn, cộng các nhóm lại sẽ vượt tổng danh mục. Báo cáo BI hành xử y hệt.
  nhommucdo: ["Bồi thường CGQ", "Phân loại số tiền tổn thất"],
  loaixe_f0: ["Loại xe", "F0"],
  loaixe_f1: ["Loại xe", "F1"],
  loaixe_f2: ["Loại xe", "F2"],
  ftnds: ["Loại xe", "FTNDS"],
};

export const DIM_IDS = Object.keys(DIM_MAP);

// Chiều chỉ tồn tại phía bồi thường: một hợp đồng không có hồ sơ thì không có xưởng sửa chữa, không
// có mức độ tổn thất. Model không cắt được phí/exposure theo chúng nên mỗi nhóm nhận nguyên phí của
// cả đoạn (đo thực tế: tổng phí của 5.393 nhóm xưởng bằng 5.393 lần tổng thật). Severity vẫn đúng vì
// chỉ lấy bồi thường chia số hồ sơ; tỷ lệ bồi thường và tần suất thì không so sánh được giữa các nhóm.
// Vì vậy: vẫn cho chọn làm chiều của bản đồ nhiệt (có cảnh báo), nhưng không đưa vào lượt quét nguyên
// nhân tự động, nơi chúng luôn chiếm hết đầu bảng với tỷ trọng 100%.
export const CLAIMS_ONLY_DIMS = new Set(["nhomgara", "garatt", "nhommucdo"]);

export const METRICS = [
  { id: "lr", name: "Tỷ lệ bồi thường", formula: "Incurred / Phí thực hưởng" },
  { id: "freq", name: "Frequency", formula: "Số vụ / Exposure" },
  { id: "sev", name: "Severity", formula: "Incurred / Số vụ" },
  { id: "pp", name: "Pure Premium", formula: "Incurred / Exposure" },
  { id: "avgprem", name: "Average Premium", formula: "Phí thực hưởng / Exposure" },
  { id: "ae", name: "A/E (so trung bình toàn danh mục)", formula: "Incurred thật / (freq×sev toàn danh mục × Exposure)" },
];

// Mẫu số dùng để tính tỷ trọng đóng góp, chọn theo bản chất từng chỉ số: LR/PP/Severity/A-E quy về
// Incurred, Frequency quy về Exposure, Average Premium quy về phí thực hưởng.
export const METRIC_BASE = {
  lr: "incurred", pp: "incurred", sev: "incurred", ae: "incurred",
  freq: "exposure", avgprem: "earned",
};

export const MIN_CLAIMS_FOR_DRIVER = 5;

// Sự kiện thiên tai bị loại ở filter cấp báo cáo của "Báo cáo quản trị BH XCG".
export const STORM_EXCLUDED = ["Lụt Nam Trung Bộ", "Lụt Huế - Đà Nẵng"];

export const VCX_GROUP = "5.3. BH VCX ô tô";
