# DBV Analytics Engine

Ứng dụng phân tích tỷ lệ bồi thường bảo hiểm vật chất xe cơ giới, **chạy trên máy của từng người
dùng** (không phải dịch vụ web dùng chung). Mỗi người tự đăng nhập bằng tài khoản Microsoft Power BI
của chính mình; số liệu tính trực tiếp trên Microsoft Fabric dưới đúng phân quyền dữ liệu (RLS) của
người đó, hoặc dùng dữ liệu đã làm sạch của pipeline nội bộ.

Đọc `TIEN_DO_PHIEN_LAM_VIEC.md` để biết lịch sử quyết định (vì sao chọn kiến trúc này, những hướng
đã thử rồi bỏ) — file đó là nhật ký làm việc, còn file này là hướng dẫn chạy/đóng góp.

## Kiến trúc

- `server.py` — backend Python thuần (`http.server`, không framework). Toàn bộ API, tính toán, phiên
  đăng nhập theo từng trình duyệt.
- `ui.html` — giao diện, một file HTML/CSS/JS, không build step.
- `webauth.py` — phiên (cookie) + đăng nhập Microsoft (authorization-code/PKCE, dự phòng device-code).
- `engine.py`, `fabric_extract.py`, `p2_prep.py` — bản vendor của công thức tính (nguồn chân lý ở
  `pipeline/`), copy lại đây để đóng gói `.exe` độc lập được.

## Chạy trên máy (dev)

Cần Python 3.13.

```bash
pip install -r requirements.txt
```

Xin file `fabric_config.json` (4 ID: tenantId/clientId/workspaceId/datasetId — không phải mật khẩu,
nhưng gửi riêng, không đưa lên git) từ người đã có, đặt cạnh `server.py`. Không có file này thì mọi
nguồn khác (CSV, Cache) vẫn chạy được, chỉ mất tuỳ chọn Kết nối Fabric.

```bash
python server.py
```

App tự mở `http://localhost:8787`. Cửa sổ console phải để nguyên, đóng nó là tắt app.

## Đóng gói bản `.exe` gửi người dùng cuối

```bash
build_exe.bat
```

Ra `dist/DBV_Analytics_Engine.exe`, một file duy nhất, không nhúng dữ liệu (cache chỉ sinh ra khi
người dùng bấm "Kéo dữ liệu từ Fabric về").

## Đóng góp

- Nhánh `main` là bản ổn định, `dev` là nơi làm việc chung. Tạo nhánh riêng từ `dev` cho mỗi việc lớn
  (ví dụ `feature/xuat-bao-cao`), mở Pull Request vào `dev` khi xong, merge `dev` → `main` khi đã test
  trên máy thật.
- `server.py`/`ui.html` không có test tự động — trước khi mở PR, tự chạy app và thử tay luồng vừa sửa
  (đăng nhập, một nguồn dữ liệu bất kỳ, một lượt phân tích).
- Không commit `fabric_config.json`, `fabric_token_cache.bin`, hay bất kỳ file `.parquet` nào —
  `.gitignore` đã chặn sẵn, nhưng kiểm tra `git status` trước khi `git add -A` để chắc chắn.
