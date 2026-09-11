# Bản web tĩnh — DBV Analytics Engine

Chạy hoàn toàn trong trình duyệt. Không có server, không có backend: trang tự đăng nhập Microsoft
bằng MSAL.js rồi gọi thẳng `executeQueries` của Power BI. Nhờ vậy host được trên Netlify gói miễn phí.

Phần Python trong thư mục cha (`server.py`, `engine.py`…) là bản app cũ chạy cục bộ, độc lập với
thư mục này.

## Vì sao gọi thẳng được

`api.powerbi.com` bật CORS cho mọi origin. Đã kiểm chứng: preflight từ một origin Netlify trả về
`Access-Control-Allow-Origin` đúng origin đó. Token lấy bằng authorization-code + PKCE, lưu trong
localStorage của chính người dùng; RLS vẫn do Power BI áp theo tài khoản đăng nhập.

## Cấu trúc

| File | Vai trò |
|---|---|
| `index.html` | Khung trang |
| `config.js` | Bốn ID Fabric. Không phải bí mật: clientId/tenantId lộ trong URL đăng nhập, workspaceId/datasetId lộ trong URL báo cáo |
| `js/model.js` | Bản đồ chiều, danh sách chỉ số, hằng số nghiệp vụ |
| `js/dax.js` | Dựng câu DAX. Thuần chuỗi, không mạng, nên đối chiếu được bằng script |
| `js/metrics.js` | Toán nghiệp vụ: chỉ số, top N, bridge, đóng góp của từng nhóm |
| `js/fabric.js` | Đăng nhập MSAL + gọi Power BI |
| `js/query.js` | Ghép hai lớp trên, cache trong phiên |
| `js/app.js` | Giao diện |

## Kiểm chứng số liệu

Hai script chạy cạnh nhau, so từng con số giữa bản JavaScript này và bản Python đang dùng:

```bash
# 1. lấy access token từ phiên đăng nhập đã lưu của bản Python
cd ..
py -3.13 -c "import webauth as WA, fabric_extract as F; s,_=WA.get_or_create(None); s['cache']=open('fabric_token_cache.bin',encoding='utf-8').read(); print(WA.access_token(F,s),end='')" > token.txt

# 2. chạy bản JavaScript
cd web
node verify_vs_python.mjs "$(cat ../token.txt)" > js_out.json

# 3. chạy bản Python trên đúng tham số đó rồi so
py -3.13 verify_vs_python.py js_out.json
```

Lần chạy ngày 11/09/2026 (kỳ 2026-01-01 đến 2026-07-31): khớp tuyệt đối trên toàn danh mục, 54 ô của
lưới Kênh × Giá trị xe, coverage, một đoạn đào sâu, bridge, 10 driver và phép dịch kỳ YoY/MoM.

## Hai giới hạn của model dữ liệu, đã xử lý

**Chiều "Nhóm thời gian sử dụng xe" bị bỏ.** Cột nằm trên chính bảng doanh thu `DT kế toán`, không có
đường liên kết sang hai bảng bồi thường. Cắt theo nó thì phí chia đúng nhưng bồi thường giữ nguyên của
cả đoạn và bị đếm lại ở từng nhóm: đo thực tế tổng bồi thường của 6 nhóm bằng đúng 6 lần tổng thật,
tỷ lệ bồi thường vọt lên 3.152%. Bản Python hiện vẫn còn lỗi này.

**Ba chiều chỉ có bên bồi thường** (Phân nhóm SH/GR sửa chữa, Gara sửa chữa, Nhóm mức độ tổn thất):
model không chia phí theo chúng nên mỗi nhóm nhận nguyên phí của cả đoạn — tổng phí của 5.393 nhóm
xưởng bằng 5.393 lần tổng thật. Vẫn cho chọn làm chiều của bản đồ nhiệt kèm cảnh báo, nhưng không đưa
vào lượt quét nguyên nhân tự động, nơi chúng luôn chiếm hết đầu bảng với tỷ trọng 100%.

## Chạy trên máy

```bash
cd web
py -3.13 -m http.server 8788 --bind 127.0.0.1
```

Mở http://localhost:8788. Đăng nhập được thì cần redirect URI `http://localhost:8788/` đăng ký trong
App Registration, platform **Single-page application**.

## Đưa lên Netlify

Netlify đọc `netlify.toml` ở gốc repo, publish thư mục `web`. Sau khi có domain, đăng ký thêm redirect
URI `https://<domain>/` cũng ở platform **Single-page application**.
