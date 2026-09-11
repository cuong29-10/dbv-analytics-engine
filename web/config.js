// Bốn ID của kết nối Fabric. KHÔNG phải bí mật: clientId/tenantId lộ ra trong chính URL đăng nhập
// Microsoft, workspaceId/datasetId lộ ra trong URL báo cáo trên app.powerbi.com. Quyền truy cập dữ
// liệu do Entra ID và RLS của Power BI quyết định, không do việc giấu mấy ID này.
window.DBV_CONFIG = {
  tenantId:    "c2284dc2-34b2-4495-85cd-1bd1eb95e332",
  clientId:    "6c8a9458-eca4-4a7d-bbd2-642bd317f3b8",
  workspaceId: "3caae101-cd9e-4e9a-b0b2-8b79c0e55a62",
  datasetId:   "672b3c7b-5362-4ffe-b191-155d91ee7678",
};
