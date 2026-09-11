# Tiến độ phiên làm việc — Live Connection & các việc liên quan

Ghi lại để nối tiếp ở phiên chat mới mà không mất ngữ cảnh. Phạm vi làm việc chỉ trong
`Xây dựng prototype phân tích số liệu/app/` (app dev/test) và `pipeline/` (dùng chung, hầu như
không đụng tới trong phiên này). `p1_extract.py` **không được sửa** vì dùng chung cho dự án khác.

## 1. Việc đã xong, đã kiểm chứng

### 1.1. Sửa lỗi tốc độ kéo dữ liệu Fabric (đầu phiên)
- Rút gọn `POL_COLS`/`CGQ_COLS`/`DGQ_COLS` trong `fabric_extract.py` (cả bản `pipeline/` và bản vendor
  trong `app/`) — chỉ bỏ cột không dùng tới, đã kiểm chứng không đổi số ra.
- **Không đụng `p1_extract.py`.**
- Thử cơ chế kéo song song rồi **rollback hoàn toàn** theo yêu cầu (bên dùng Power BI Pro, không phải
  Premium, sợ nghẽn tài nguyên dùng chung) — không còn dấu vết trong code.

### 1.2. Live Connection tới Microsoft Fabric — tính năng chính của phiên này

**Ý tưởng:** thay vì kéo cả lịch sử về máy tính lại (nguồn "Kéo dữ liệu từ Microsoft Fabric về để
phân tích", mất hơn 1 tiếng), gọi thẳng các **measure DAX có sẵn trong file .pbix** của báo cáo qua
Fabric REST API (`executeQueries`) mỗi khi đổi chỉ số/kỳ/chiều. Đây là nguồn mới, độc lập, không đụng
gì tới nguồn kéo-toàn-bộ hay Cache.

**Đường đã thử và bỏ:** kéo dữ liệu thô đúng kỳ đang xem rồi tính bằng `engine.py` cục bộ. Bỏ vì bảng
`"DT kế toán"` là bảng **chi tiết kế toán** (nhiều dòng bút toán share chung ngày hiệu lực hợp đồng),
lọc theo cửa sổ ngày gần như không giảm được số dòng khớp — đo thực tế ngay cả 1 ngày cũng khớp
~362.000 hợp đồng, vượt xa giới hạn phản hồi 15MB của Fabric. Đây là giới hạn cứng của nền tảng.

**Đường đang dùng:** đọc thẳng measure `[Đếm số hợp đồng]`, `[Phí thực hưởng]`, `[CPBT gốc năm nay]` +
đếm hồ sơ qua `DISTINCTCOUNT`, group theo `SUMMARIZECOLUMNS` theo đúng bảng/cột chiều tương ứng trong
model Fabric (`LIVE_DIM_MAP`). Freq/Severity/Phí BQ/LR app **tự tính** từ 4 số thô này (không đọc
measure LR/Freq/Sev có sẵn), để công thức nhất quán với `cell_metrics()` ở đường kéo-toàn-bộ.

**Bài học lớn nhất của phiên này — CHUẨN ĐỐI CHIẾU PHẢI LÀ BÁO CÁO POWER BI, không phải pipeline.**
Ban đầu tôi lấy pipeline (`clean_policy.parquet` + `engine.py`) làm chuẩn, thấy Live lệch nhẹ nên đi
sửa lệch đó (bù +1 ngày, chia tỷ lệ đồng khai thác qua `Tỷ lệ ghi nhận`) — nhưng người dùng chỉ ra
đúng: người xem sẽ đối chiếu với **báo cáo BI**, không phải pipeline. Kiểm tra lại thì phát hiện:
- App tự tính (từ Exposure/Earned/Incurred/Claims thô) **khớp tuyệt đối (lệch 0,000000)** với measure
  gốc của báo cáo trên **mọi nhóm của mọi chiều** — kể cả chiều Đơn vị vốn trước đó bị tưởng là hỏng.
- Lý do trước đó tưởng Đơn vị hỏng: so với pipeline thì lệch (pipeline dùng quy ước đếm ngày khác,
  +1 ngày ở `engine.add_days`), nhưng so với báo cáo BI thì **không hề lệch**.
- Kết luận: **bỏ hết** hướng "bù +1 ngày" / "chia tỷ lệ đồng khai thác" — hai việc đó làm Live khớp
  pipeline nhưng lệch khỏi báo cáo BI, đi ngược đúng yêu cầu người dùng.
- Hệ quả đã biết và được chấp nhận: Live (khớp báo cáo BI) và nguồn kéo-toàn-bộ trong chính app
  (khớp pipeline) sẽ lệch nhau ~0,4-0,8% do khác quy ước đếm ngày. Muốn hai nguồn thống nhất phải sửa
  ở pipeline, không sửa ở Live.

**Phát hiện quan trọng khác trong lúc soát báo cáo BI:**
- Bộ lọc cấp báo cáo (đọc thẳng từ `Report/definition/report.json` trong .pbix) có lọc nghiệp vụ VCX
  ô tô, nguồn dữ liệu `DBV`, và loại trừ 2 sự kiện bão (`'Lụt Nam Trung Bộ'`, `'Lụt Huế - Đà Nẵng'`).
  Measure DAX **không tự lọc** các điều kiện này trong công thức — Power BI áp qua "Filter on this
  page", một filter context chỉ tồn tại trong report, không đi kèm khi gọi measure thẳng qua
  executeQueries. Live phải tự dựng lại các bộ lọc này (`_live_vcx_filters()`/`LIVE_STORM_EXCLUDED`).
- File .pbix có **RLS thật, kiểu động**: 3 role (Đơn vị/Kênh/Nghiệp vụ) lọc theo
  `[Email phân quyền] = USERPRINCIPALNAME()`, khớp với 3 bảng phân quyền (108/9/125 dòng). Đăng nhập
  đúng tài khoản thì Fabric tự cắt đúng dữ liệu người đó được xem — không cần app tự viết logic phân
  quyền. (Việc này chỉ liên quan tới bàn "lên web" ở mục 3, chưa cần code gì.)

**Số liệu đối chiếu cuối cùng (kỳ 2026-01-01 → 2026-07-31):**
- LR toàn danh mục: **65,3387%** (đúng bằng báo cáo BI)
- Exposure: 174.162,1 | Earned: 1.321.215.085.802 | Incurred: 863.264.717.818 | Claims: 118.832
- Đối chiếu trên **27 chiều, 12.287 nhóm** — lệch 0,000000 trên LR/Freq/Severity/Phí BQ ở mọi nhóm.

## 2. Trạng thái code hiện tại (đã làm, đã test, CHƯA đụng bản standalone)

File đã sửa: `server.py`, `ui.html`. Chưa đụng `fabric_extract.py`, `p2_prep.py`, `engine.py`,
pipeline, hay build standalone.

- **`LIVE_DIM_MAP`**: 27/28 chiều dùng được (trước phiên này chỉ 20/28). Chỉ còn `dongco` (Động cơ)
  tắt vì bảng nguồn rỗng trên Fabric, báo lỗi rõ ràng (`_live_dim_unsupported_msg()`), không phải lỗi
  chung chung.
- **Bộ lọc `_live_vcx_filters()`**: đã thêm điều kiện loại 2 sự kiện bão (`LIVE_STORM_EXCLUDED`).
- **`ui.html`**: bỏ hiệu ứng shining (class `.live-highlight`, đã xoá sạch CSS liên quan) trên thẻ
  "Kết nối tính toán trực tiếp với Microsoft Fabric". Không gắn nhãn "đang phát triển" (yêu cầu rõ:
  KHÔNG được ghi nhãn này vì số đã khớp báo cáo). Chú thích dưới thẻ và trong panel đã cập nhật, nêu
  rõ: số lấy tới lần làm mới gần nhất của dataset (06:00 & 12:00 hằng ngày), gọi thẳng measure của
  báo cáo dưới đúng bộ lọc báo cáo đang áp.
- **`verify_live_vs_report.py`** (file mới, cạnh `server.py`): script đối chiếu độc lập, chạy lại được
  bất cứ lúc nào — `py -3.13 verify_live_vs_report.py [YYYY-MM-DD YYYY-MM-DD]`. Kiểm 2 việc: (1) bộ
  lọc app ra cùng số với bộ lọc đọc từ .pbix; (2) LR/Freq/Sev/Phí BQ app tự tính khớp measure gốc trên
  từng nhóm của từng chiều. Ngưỡng: 0,5 điểm cho % , 0,5% cho số tiền — thực tế đo được 0,000000.
- Đã hồi quy nguồn kéo-toàn-bộ (Cache/pipeline) sau khi sửa — chạy đúng như cũ, không bị ảnh hưởng
  (LR 65,0956%, Exposure 175.538,1, đủ 29 chiều, Đào sâu/Decomposition Tree bình thường).
- Dev server đang chạy trên máy, cổng 8787 — token đổi mỗi lần khởi động lại (xem log lúc mở app để
  lấy link mới nếu server đã tắt).

### Việc CHƯA làm (đã dừng lại theo đúng yêu cầu "chưa đụng bản standalone")
- Bản standalone (.exe) **chưa được cập nhật/đóng gói lại** với các sửa đổi trên. Người dùng đã tự
  test bản dev/test trên máy trước, thấy số khớp báo cáo BI thì mới triển khai tiếp sang standalone.
- Trước đó có bàn việc: khi đưa sang standalone thì bỏ hiệu ứng shining + gắn nhãn "đang phát triển"
  + chú thích nêu rõ chưa loại filter-on-page — **nhưng yêu cầu này đã bị thay thế** bởi yêu cầu mới
  hơn (đối chiếu 0,5 điểm với báo cáo BI). Giờ số đã khớp thật nên **không cần** nhãn "đang phát
  triển" nữa, chỉ cần đưa đúng các sửa đổi ở mục 2 sang standalone khi được duyệt.

## 3. Chuyển sang bản web — ĐÃ CODE XONG phần lõi, đang chờ test và redirect URI (11/09/2026)

Quyết định đã chốt: bỏ hướng app/.exe cho người dùng cuối, làm bản web, đưa lên GitHub và host trên
**Render** (gói Free, không cần thẻ). Netlify bị loại vì không chạy được backend Python. Bản standalone
`.exe` hiện có KHÔNG đụng tới, vẫn chạy như cũ.

### 3.1. Đã làm
- **`webauth.py`** (mới): phiên theo cookie `dbv_sid` (HttpOnly, SameSite=Lax, Secure khi https, hết hạn
  sau 12 giờ không dùng), token cache MSAL riêng từng phiên, đăng nhập authorization-code + PKCE
  (`/auth/login` → Microsoft → `/auth/callback`), `/auth/logout`. Device-code giữ làm đường phụ khi chạy
  localhost (`/api/fabric_login_start` + `_wait`), cũng ghi vào cache của phiên. Lớp `SessionScoped` thay
  các biến toàn cục cũ mà không đổi cách gọi.
- **`server.py`**: `STORE`, `SLICE_CACHE`, `ANALYZE_CACHE`, `LIVE_GRID_CACHE`, `LIVE_ANALYZE_CACHE`,
  `_LIVE_VCX_CACHE` giờ là `SessionScoped` (mỗi phiên một bản; bản standalone không có phiên nên rơi về
  dict toàn cục như trước). Bỏ hẳn `OWNER_TOKEN`/`GUEST_TOKEN`/`token_role`/`OWNER_ONLY_PATHS`, bỏ
  `h_run_extract`/`h_pbix_scan`/import `p1_extract`. Handler mở phiên đầu mỗi request, đặt token Fabric
  của phiên vào `fabric_extract.SESSION_TOKEN` (ContextVar) nên `run_dax` tự dùng đúng token người đang
  xem. `PORT` đọc từ biến môi trường. Hàm chạy chính là `run_web()`: bind `127.0.0.1` khi chạy local,
  `0.0.0.0` khi có `DBV_BASE_URL` (host).
- **`fabric_extract.py`** (bản vendor trong app/, bản pipeline/ KHÔNG đổi): thêm `SESSION_TOKEN`
  ContextVar; `run_dax` ưu tiên token phiên, rơi về token cache file (CLI/pipeline vẫn chạy như cũ);
  `load_config()` đọc 4 ID từ biến môi trường `FABRIC_TENANTID/CLIENTID/WORKSPACEID/DATASETID` nếu có.
- **`ui.html`**: bỏ thẻ .pbix, bỏ Chia sẻ link và chế độ guest, bỏ token trong URL. Thẻ Live Fabric lên
  đầu, có vệt sáng chạy qua (`.live-card`), nút "Đăng nhập Microsoft" nằm trong panel Live. Icon các thẻ
  đổi từ emoji sang SVG cùng cỡ, nằm đúng tâm ô. Chip tài khoản ở topbar (email + Đăng xuất). Màn chặn
  "Đăng nhập để tiếp tục" hiện khi server bắt buộc đăng nhập mà chưa có tài khoản; mọi 401 từ API cũng
  mở màn này.
- `requirements.txt`, `render.yaml` (Blueprint cho Render), `.gitignore` riêng cho thư mục app.
  Xoá `app/p1_extract.py` (không còn gì import).

Biến môi trường bản web: `PORT` (Render tự đặt), `DBV_BASE_URL` (https://<domain>, quyết định redirect
URI và cờ Secure của cookie), `DBV_LOGIN_REQUIRED` (mặc định 1 khi có DBV_BASE_URL, 0 khi chạy local),
`FABRIC_TENANTID`, `FABRIC_CLIENTID`, `FABRIC_WORKSPACEID`, `FABRIC_DATASETID`.

### 3.2. Đã kiểm chứng trên máy
- Hai phiên (hai cookie) nạp dữ liệu độc lập: phiên 1 nạp cache thấy `hasData=true`, phiên 2 vẫn
  `hasData=false` và `/api/analyze` báo chưa có dữ liệu.
- Token theo phiên gọi Fabric thật (gieo phiên bằng token cache có sẵn trên máy): `fabric_account()`
  trả đúng `baocaobi@dbvi.com.vn`, `h_fabric_live_analyze` kỳ 2026-01→07 chạy xong, không lỗi.
- Mô phỏng chế độ host (`DBV_BASE_URL=https://...`): `/api/analyze` trả 401 khi chưa đăng nhập, cookie
  có `Secure`, device-code bị chặn, `/auth/login` chuyển đúng tới login.microsoftonline.com với
  `redirect_uri=https://<domain>/auth/callback`.

### 3.2b. Sửa thêm sau lần rà giao diện (11/09/2026)
- Bỏ hai dòng chú thích đầu trang, bỏ toàn bộ foot-note dài của panel Live, bỏ hẳn nguồn "Đồng bộ từ
  máy chia sẻ" (cả thẻ, panel, `h_sync_pull`, `sync_config.json`) vì bản web không còn chia sẻ máy.
- Wording thẻ Live và thẻ Kéo dữ liệu về theo đúng câu người dùng chốt.
- Thêm `/api/fabric_last_refresh`: đọc `/refreshes` của Power BI REST (cùng quyền `Dataset.Read.All`),
  hiện "Số liệu trên báo cáo tới thời điểm HH:MM ngày DD/MM/YYYY" dưới panel Live sau khi đăng nhập.
- **Lỗi tốc độ đã sửa**: server chỉ bind `127.0.0.1`, trong khi Windows phân giải `localhost` sang `::1`
  trước — mỗi request phải chờ IPv6 thất bại. Đo được 2,05 giây/request qua `localhost` so với 0,03 giây
  qua `127.0.0.1`. Nay nghe cả hai (thêm `ThreadingHTTPServer6` trên `::1`), đo lại còn 0,03 giây.
  Redirect URI đã đăng ký là `localhost` nên bắt buộc phải xử lý, không thể né bằng cách đổi địa chỉ.

### 3.2c. Đo tải nhiều người dùng đồng thời
- 20 request song song qua `localhost`: tổng 0,05 giây, chậm nhất 0,02 giây. `ThreadingHTTPServer` mỗi
  request một luồng, phần lớn thời gian là chờ Fabric trả lời nên GIL không phải nút thắt.
- RAM mỗi phiên khi dùng nguồn **Cache**: giữ 69 MB (đỉnh lúc nạp 91 MB) cho 881.068 dòng hợp đồng +
  507.791 dòng bồi thường. Render Free có 512 MB, nên khoảng 4-5 phiên cùng dùng nguồn Cache là chạm
  trần. Nguồn **Live Fabric** chỉ giữ lưới kết quả, không đáng kể — đây là lý do đặt Live làm mặc định.
- Phiên tự hết hạn sau 12 giờ không dùng (`SESSION_IDLE_SECONDS`), dọn trong `_purge_idle()`.

### 3.3. Chưa làm / chờ
1. **IT đăng ký redirect URI** (Phần 3.4). Chưa có thì nút "Đăng nhập Microsoft" sẽ bị Microsoft báo lỗi
   `AADSTS50011`; trên máy tạm dùng nút "Đăng nhập bằng mã (thử trên máy)".
2. Người dùng tự test bản local qua UI: đăng nhập, Live Fabric, Cache, CSV, Kéo Fabric về, Đào sâu.
3. Repo git đã khởi tạo trong `app/` (nhánh `main` + `dev`, commit đầu xong). Remote GitHub:
   `https://github.com/cuong29-10/dbv-analytics-engine.git` — CHƯA push, chờ người dùng duyệt giao diện.
   Sau đó nối Render qua Blueprint (không đẩy cả repo báo cáo có .pbix/.xlsx/.pptx), nhánh
   `dev` để làm việc, `main` để Render build. Nối Render qua Blueprint (`render.yaml`), điền 4 ID Fabric
   vào Environment, sửa `DBV_BASE_URL` theo domain Render cấp, rồi gửi domain đó cho IT thêm redirect
   URI thứ hai.
4. Chưa có giới hạn số phiên/RAM: Render Free 512MB; Live Fabric nhẹ, nguồn Cache/Kéo về nạp pandas theo
   từng phiên có thể chạm trần khi nhiều người cùng nạp. Nếu gặp thì chuyển Hugging Face Spaces (16GB).

### 3.4. Hướng dẫn cho IT (một lần, khoảng 5 phút, không cần secret)
portal.azure.com → Microsoft Entra ID → App registrations → mở app có Client ID trùng `clientId` trong
`fabric_config.json` → **Authentication** → **Add a platform** → **Mobile and desktop applications** →
mục "Custom redirect URIs" thêm:
- `http://localhost:8787/auth/callback`
- `https://<domain-render>/auth/callback` (bổ sung sau khi có domain)

Save. Kiểm tra lại "Allow public client flows" vẫn là **Yes**. Permission `Dataset.Read.All` (Power BI
Service, delegated) đã có từ trước, không cần thêm. Không cần tạo client secret. IT chỉ cần báo "đã thêm".

## 3 (cũ). Thảo luận kiến trúc trước khi code
 — chưa code, chỉ mới bàn (app hay web)

Người dùng hỏi "host lên web" thay vì làm app — đã trả lời đầy đủ, **chưa làm gì**, chờ quyết định.

**Kết luận đưa ra:** đề xuất chuyển sang web, vì mục đích thật là nhiều người xem, và phần lớn rắc
rối gặp phải trong app (token đổi mỗi lần mở lại, chung một tài khoản Fabric cho cả máy, phải đóng
gói lại .exe mỗi khi sửa) là rắc rối của *hình thức app*, không phải của bài toán phân tích.

**Từng nguồn dữ liệu khi lên web:**
- Fabric (kéo-về & Live): làm được, hợp web hơn cả app — chỉ cần đổi kiểu đăng nhập.
- CSDL (SQL Server/DWH): làm được tự nhiên, hiện chưa có CSDL nào phía sau nên chưa cần.
- File .pbix desktop: **không làm được** trên web (trình duyệt chặn đọc file máy khách) — nên bỏ,
  vì Fabric đã giữ đúng model đó rồi.
- CSV upload: được. Đồng bộ LAN: hết ý nghĩa (server web chính là đầu mối).

**Hai việc bắt buộc phải đổi nếu lên web** (đã giải thích kỹ, dùng đúng tên biến/hàm trong code):
1. **Đăng nhập theo từng người**: hiện `fabric_extract.py` dùng MSAL `PublicClientApplication` +
   device-code flow, token lưu **một file chung** (`fabric_token_cache.bin`) cho cả server. Lên web
   phải đổi sang authorization-code flow, mỗi người tự đăng nhập Microsoft trên trình duyệt, token
   giữ riêng theo phiên của người đó.
2. **Tách trạng thái theo phiên**: hiện `server.py` giữ `STORE`, `LIVE_GRID_CACHE`,
   `LIVE_ANALYZE_CACHE` là biến toàn cục dùng chung mọi request — một người đổi nguồn/kỳ sẽ ảnh
   hưởng người khác đang xem. Lên web phải tách theo session (cookie), không dùng chung 1 biến nữa.

**Chi phí:** đăng nhập theo từng người (authorization-code) và device-code hiện tại đều dùng chung
Microsoft Entra ID miễn phí, không phát sinh phí, không cần nâng license. Chi phí chỉ ở phần host
(máy chủ chạy quanh năm), tách biệt với chuyện đăng nhập.

**RLS (đã kiểm chứng bằng cách đọc thẳng .pbix, xem mục 1.2):** 3 role động, khớp email đăng nhập với
3 bảng phân quyền có sẵn. Đăng nhập đúng tài khoản, Fabric tự áp đúng phần dữ liệu người đó được xem
— khớp đúng suy luận của người dùng.

**Có cần nhờ IT không:** chủ yếu là code. Một việc không phải code: đăng ký **redirect URI** (và có
thể thêm client secret) cho app registration hiện tại (`clientId` trong `fabric_config.json`) trên
Azure Portal — vài phút thao tác, không phải dự án lớn. Ai đang có quyền Application Administrator
trong Entra ID của công ty tự làm được, không nhất thiết phải qua IT hạ tầng.

## 4. Việc cần làm tiếp (khi mở phiên mới)

0. Bản web: theo mục 3.3. Chạy `py -3.13 server.py` rồi mở http://localhost:8787 (không còn token trong link).
1. Chờ người dùng tự test bản dev trên máy.
2. Nếu người dùng xác nhận số khớp báo cáo BI: đóng gói các sửa đổi ở mục 2 sang **bản standalone**
   (rebuild .exe) — nhớ **không** gắn nhãn "đang phát triển" theo yêu cầu mới nhất.
3. Quyết định kiến trúc app-vs-web (mục 3) vẫn đang mở, chưa có hành động nào được yêu cầu.
4. Nếu có thời gian rảnh và được yêu cầu: có thể xem xét đồng bộ cách tính +1 ngày giữa Live (khớp
   báo cáo BI) và pipeline (khớp engine.py) để hai nguồn trong app thống nhất — nhưng đây là việc sửa
   ở pipeline, chưa được yêu cầu, không tự làm nếu chưa hỏi lại.
