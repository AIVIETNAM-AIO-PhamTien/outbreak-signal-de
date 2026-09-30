# Lỗi Bronze phát hiện khi dựng Silver/Gold (29–30/09/2026)

Các lỗi dưới đây lộ ra khi dùng Bronze gốc làm đầu vào thật cho Silver/Gold thử độ khả thi ở
nhánh `feat/silver-gold` (commit `001817c`). Các bản sửa đã được đưa sang nhánh
`fix/ingest_to_bronze`; cột "Đã sửa ở" chỉ file chứa bản sửa. Mỗi lỗi đều có bằng chứng đo trên
dữ liệu thật.

Mức độ:
- **Cao:** Silver/Gold ra kết quả sai hoặc mất dữ liệu.
- **Trung bình:** chạy được nhưng dễ vỡ.
- **Thấp:** tiện vận hành.

## A. Lỗi của code Bronze (cần sửa ở repo gốc)

| # | Mức | Nguồn | Lỗi | Bằng chứng | Cách sửa đã chọn | Đã sửa ở |
|---|---|---|---|---|---|---|
| 1 | Cao | news_rss | Chỉ có 1 truy vấn tiếng Anh `dengue Southeast Asia` (gl=US), không giới hạn thời gian → toàn tin cũ, không "sớm" | 0/64 bài đăng trong 7 ngày; chỉ 23% bài gắn được nước | 11 feed theo nước, ngôn ngữ bản xứ (vi, th, id, ms…), thêm `when:7d` vào `q`. KH/LA/MM dùng `en` vì locale km/lo/my trả 0 bài | `configs/sources.yaml` (`news_rss.feeds`), `ingestion/news_rss.py` (`feed_params`) |
| 2 | Cao | news_rss | Bỏ mất `guid`, khoá tự nhiên của bài; chỉ giữ 5 trường | Silver phải dedup theo `link`; `link` của Google News có thể đổi | Giữ whitelist trường + `guid` + `raw_payload` (XML gốc của item) + `source_url`, `feed_country/hl/gl/query` | `ingestion/news_rss.py` (`parse_items`) |
| 3 | Cao | news_rss, who_gho | `_ingested_at` bị **ghi đè** mỗi lần chạy lại trong cùng ngày, vì cả partition ngày được ghi lại từ mọi file landing | Mọi dòng trong một partition có cùng `_ingested_at` → không biết bài được thấy lần đầu lúc nào | Thêm `_fetched_at` vào từng bản ghi **lúc gọi API** (trong file landing), không phụ thuộc lúc ghi Delta | `news_rss.py`, `who_gho.py` |
| 4 | Cao | opendengue | Partition theo `ingestion_date` → **ngày nào cũng tải lại và ghi lại toàn bộ snapshot** (~70k dòng) dù release không đổi | Bronze phình theo số ngày chạy; Silver phải tự lọc trùng | Partition theo `release`; bỏ qua nếu `_file_sha` (git blob sha từ GitHub API) đã có trong Bronze | `ingestion/opendengue.py` (`resolve_release`, `ingest`), `common/bronze.py` (`ingested_rows`) |
| 5 | Cao | opendengue | URL cứng tới `V1.3` → release mới ra sẽ không bao giờ được lấy | — | Tìm release mới nhất qua GitHub API (`releases_api` trong config), chọn file `Spatial` | `opendengue.py` (`parse_release`, `pick_latest_release`, `find_extract`) |
| 6 | Cao | tất cả | Có `retries: 3` trong config nhưng **không code nào dùng**; `requests.get` gọi thẳng, lỗi mạng hay 5xx làm fail cả lần chạy | Code gốc không đọc `retries` | Module HTTP chung: retry (tenacity) khi ConnectionError/Timeout/429/5xx, không retry 4xx; tải file dạng stream ra `.part` rồi mới đổi tên | `ingestion/common/http.py` |
| 7 | Trung bình | who_gho | Để Spark tự suy kiểu JSON → cột toàn null (`POPULATION`) thành string; hôm nào có số sẽ đổi kiểu và **vỡ bước ghi Delta** | `POPULATION` toàn null ở 1.232/1.232 dòng | Đọc `primitivesAsString=true`: Bronze toàn string, ép kiểu để Silver làm | `who_gho.py` (`load_to_bronze`) |
| 8 | Trung bình | who_gho | Chạm `$top` chỉ log cảnh báo → lần chạy vẫn SUCCESS với dữ liệu bị cắt | Hiện 1.232 dòng / `$top` = 10.000, chưa xảy ra | Chạm `$top` thì raise `IngestionValidationError`, không ghi | `who_gho.py` (`fetch_raw`) |
| 9 | Trung bình | news_rss, who_gho | `_source_file` là đường dẫn tuyệt đối (`file:///C:/Users/...`) → khác nhau giữa các máy, lộ tên user | — | Chỉ giữ phần sau `landing/` | `common/bronze.py` (`add_bronze_columns`) |
| 10 | Thấp | news_rss | Một feed lỗi làm hỏng cả lần chạy; không biết feed nào chạm trần 100 bài | IDN, PHL, SGP đều trả đúng 100 bài | Mỗi feed chạy độc lập; lỗi và chạm trần ghi vào `meta.warnings` | `news_rss.py`, `common/metadata.py` |
| 11 | Thấp | tất cả | Không có trạng thái "bỏ qua vì không có gì mới"; lỗi khi ghi metadata trong `finally` **che mất lỗi gốc** | — | `meta.skip(reason)`, trạng thái SKIPPED trong báo cáo `run_batch`; `write_or_log()` trong `finally` | `common/metadata.py`, `scripts/run_batch.py` |

**Lưu ý chuyển đổi:** sửa #4 (đổi cột partition của OpenDengue) và #7 (WHO toàn string) làm bảng
Delta cũ **không tương thích**. Code giờ tự phát hiện trường hợp này (`check_existing_table`, xem
#16) và báo lỗi rõ ràng. Cách xử lý: chuyển thư mục bảng cũ trong `data/bronze/` sang một thư mục
backup rồi chạy lại `run_batch.py`.

### A2. Lỗi phát hiện ở lượt review thứ hai (30/09/2026)

Tất cả đã sửa, mỗi lỗi có test tái hiện trong `tests/test_integration_reruns.py` hoặc
`tests/test_ingestion_units.py`.

| # | Mức | Nguồn | Lỗi | Cách sửa |
|---|---|---|---|---|
| 12 | Cao | trends_th | Chỉ kiểm tra bảng `weekly` (ghi trước). Nếu ghi `province` lỗi thì mọi lần sau đều SKIPPED, `province` rỗng mãi | Chỉ bỏ qua khi **mọi** bảng đã có `_release` + `_file_md5` này |
| 13 | Cao | hdx_cod_ab | Tương tự: polygon VN (ghi sau bảng đơn vị) lỗi thì không bao giờ được thử lại | Kiểm tra riêng `hdx_cod_ab_geometry`; bảng đơn vị có rồi thì chỉ nạp lại polygon, không tải lại XLSX |
| 14 | Cao | sg_nea | 0 cụm dịch → SKIPPED, không ghi gì → snapshot cũ trong ngày vẫn báo các cụm đã hết | 0 cụm là một quan sát: xoá `jsonl` cũ và các dòng của partition ngày, status SUCCESS với 0 dòng |
| 15 | Trung bình | tất cả (tải file) | `ChunkedEncodingError` / `ContentDecodingError` (đứt kết nối **giữa** lúc tải) không được retry | Thêm vào `is_retryable`; file `.part` được ghi lại từ đầu mỗi lần thử |
| 16 | Trung bình | opendengue, who_gho | Bảng cũ không tương thích → tải lại 55MB mỗi ngày rồi fail với `AnalysisException` khó hiểu | `check_existing_table()` trong `write_bronze` (và trước khi tải với OpenDengue) báo lỗi rõ, kèm hướng dẫn |
| 17 | Trung bình | news_rss | XML thô chỉ được lưu **sau** khi parse OK → mất bản gốc đúng lúc cần (Google trả trang captcha). Trang HTML hợp lệ XML thì thành "0 bài" im lặng | Lưu XML ngay khi nhận response; thẻ gốc khác `<rss>` → lỗi feed, có ghi warning |
| 18 | Thấp | hdx_cod_ab, hdx_cod_ps | SKIPPED báo "0 dòng" dù bảng có 680 / 14.200 dòng | Báo số dòng đang có trong bảng |
| 19 | Thấp | opendengue, hdx, trends_th, ph_doh | CSV đọc theo dòng vật lý: ô có xuống dòng bị cắt thành 2 dòng lệch cột. OpenDengue V1.3 có 4 bản ghi như vậy (đều là Ấn Độ, nên Bronze Đông Nam Á chưa bị ảnh hưởng) | `read_csv_strings()`: `multiLine` + `escape='"'`. Đo trên file thật: 2.821.791 bản ghi, khớp parser CSV chuẩn (cách cũ ra 2.821.799); đọc chậm hơn 3 giây |

**Chưa sửa, cần nhóm quyết định:**
- **Lỗi một phần vẫn báo SUCCESS.** Nếu 8/9 nước của `hdx_cod_ab` lỗi, hoặc 10/11 feed news
  lỗi, lần chạy vẫn là SUCCESS (lỗi chỉ nằm trong `warnings`) và exit code là 0.
- Cần chọn ngưỡng: vượt quá bao nhiêu phần lỗi thì coi là FAILED.

## B. Nguồn mới đã thêm (để có dữ liệu cấp tỉnh, chỉ 11 nước)

| Bảng Bronze | Nguồn | Nội dung | Idempotency |
|---|---|---|---|
| `hdx_cod_ab` (+ `_geometry` cho VNM) | HDX COD-AB (XLSX) | Đơn vị hành chính: P-code, tên, tên bản xứ, toạ độ tâm. 9 nước (không có SGP, BRN) | `last_modified` → `_version` |
| `hdx_cod_ps` | HDX COD-PS | Dân số cấp 1 (PHL cấp 2) | `last_modified` |
| `trends_th_weekly`, `trends_th_province` | Zenodo (TRENDS) | Thái Lan, tuần × 77 tỉnh, 2016–2025 | record id + md5 |
| `ph_doh` | HDX (DOH-Epi) | Philippines, tuần × tỉnh; tháng đầy đủ cuối cùng 12/2020 (tên file ghi 2016-2021) | `last_modified` |
| `sg_nea` | data.gov.sg | Cụm dịch Singapore | theo ngày |

Không thêm:
- **Malaysia iDengue:** không có API; muốn lấy phải scrape, vi phạm quy tắc dự án.
- **GeoJSON COD:** file quá lớn (PHL 1,06 GB). Dùng XLSX + polygon Natural Earth thay thế.

## C. Vấn đề của chính nguồn dữ liệu (KHÔNG sửa ở Bronze, Silver đã xử lý)

Bronze giữ nguyên dữ liệu thô. Các điểm dưới đây ghi lại để người làm Silver biết.

| Nguồn | Vấn đề | Số đo | Silver xử lý |
|---|---|---|---|
| OpenDengue | `UUID` là mã **tài liệu nguồn**, không phải khoá dòng | 234 UUID / 70.557 dòng | Không dùng làm khoá |
| OpenDengue | PH: `adm_1` = vùng, `adm_2` = tỉnh | — | PH lấy cấp 2 làm "tỉnh" |
| OpenDengue | `RNE_iso_code` của PH sai trên diện rộng | 51 địa danh có mã không khớp tên | Nối theo mã + tên, mã sai thì lùi về tên |
| OpenDengue | Số liệu cấp tỉnh dừng sớm: VN/PH/KHM/LAO tới 2010, IDN tới 07/2024 | — | Tỉnh không có số ca 24 tháng gần đây → dùng tín hiệu tin tức |
| OpenDengue | `T_res` trộn Week/Month/Year; nhiều chuỗi song song cùng tháng | 60 trường hợp | Gold chọn đúng 1 chuỗi: month > week, isoweek > epiweek, Total > … |
| OpenDengue | Địa danh ngoài phạm vi ("MONGAR" là huyện của Bhutan) | 1 | Không nối được → ghi báo cáo |
| WHO GHO | Kỳ báo cáo ở tương lai (MYS, SGP năm 2027–2029) | 5 dòng | Cờ `future_period`, loại khỏi Gold |
| WHO GHO | `START_DATE` trống (IDN 2007–2009) | 36 dòng | Suy từ `YEAR` + `DATE_NUM`, cờ `start_date_derived` |
| WHO GHO | `CONFIRMED` > `CASES` (KHM 2025) | 1 dòng | Cờ `confirmed_gt_cases` |
| WHO GHO | Khoá dòng là (ISO3, YEAR, DATE_TYPE, DATE_NUM); Indonesia nằm ở 2 WHO region | — | Dedup theo khoá này |
| WHO GHO | Tháng gần nhất chưa báo cáo đủ (IDN: T6 9.264 → T7 3.514 → T8 67) | 2 nước | Tháng giảm > 50% bị bỏ qua khi đánh giá nguy cơ |
| HDX COD-AB | IDN vẫn là bản 2020 (34 tỉnh, chưa có 4 tỉnh Papua mới); VN chỉ còn 34 tỉnh mới | — | Tỉnh cũ của VN lấy từ Natural Earth, nối cũ → mới bằng polygon |
| HDX COD | P-code của PHL khác nhau giữa COD-AB và COD-PS | — | Nối dân số PHL theo tên |
| Natural Earth | 3 tỉnh VN ghi nhầm tên **vùng** (VN-39, 53, 66); dùng chữ `Ð` (U+00D0) thay `Đ` | — | `NE_VN_NAME_FIXES`, bảng bỏ dấu |
| Google News | Bài đa ngôn ngữ (vd Vietnam.vn) xuất hiện trong feed của nước khác | — | `countries` = nước của feed ∪ nước được nhắc trong bài |

## D. Kiểm tra sau khi sửa

Đo bằng `scripts/run_transform.py` của nhánh `feat/silver-gold` → `data/quality/quality_*.md`:
- Không có ERROR.
- Test: 97 test của nhánh Bronze pass, gồm 12 test mới cho các lỗi #12–#19.
- Tin trong 7 ngày: 392/456 bài (trước khi sửa: 0/64).
- Bài gắn được nước: 89% (trước khi sửa: 23%).
- Chạy lại OpenDengue cùng release → SKIPPED, không tải lại.
