# Silver, Gold và app HealthMap (nhánh thử độ khả thi)

Phần này nằm ở nhánh `feat/silver-gold`, dùng để **thử độ khả thi** của dự án:
- dùng Bronze thật như một người tiêu thụ thật, để lộ ra lỗi Bronze;
- dựng sản phẩm cuối kiểu HealthMap ở cấp nước và cấp tỉnh cho 11 nước Đông Nam Á.

Mọi transform Silver/Gold viết bằng **PySpark**, không dùng Python UDF. App đọc Gold bằng
`deltalake`, không cần JVM.

## Chạy

```powershell
.venv\Scripts\python.exe scripts\run_batch.py        # Bronze (mọi nguồn)
.venv\Scripts\python.exe scripts\run_transform.py    # Silver + Gold + báo cáo chất lượng
.venv\Scripts\streamlit.exe run app\healthmap.py     # app, mở http://localhost:8501
```

- `run_transform.py --layer silver|gold` chạy riêng từng tầng.
- Exit code là 1 nếu có kiểm tra `ERROR`.
- Silver/Gold được ghi đè toàn bộ mỗi lần chạy, nên chạy lại cho cùng kết quả.
- Lần chạy đầu tiên tải ranh giới tỉnh Natural Earth (~40MB) về `data/reference/`.

## Thay đổi Bronze trên nhánh này

| Nguồn | Thay đổi | Idempotency |
|---|---|---|
| `news_rss` | 11 feed theo nước, ngôn ngữ bản xứ, `when:7d`; giữ `guid`, `raw_payload`, `feed_country`, `_fetched_at` | theo ngày |
| `opendengue` | tự tìm release mới nhất qua GitHub API, partition `release` | bỏ qua nếu `_file_sha` trùng |
| `who_gho` | mọi cột đọc là string; retry; chạm `$top` thì báo lỗi | theo ngày |
| `hdx_cod_ab` (mới) | đơn vị hành chính COD của 9 nước (P-code, tên, toạ độ tâm); hình VN 34 tỉnh | theo `last_modified` của HDX |
| `hdx_cod_ps` (mới) | dân số cấp 1 (cấp 2 với PHL), lọc 11 nước | theo `last_modified` |
| `trends_th` (mới) | Thái Lan, tuần × 77 tỉnh, 2016–2025 (Zenodo) | theo record id + md5 |
| `ph_doh` (mới) | Philippines, DOH tuần × tỉnh, tới 12/2020 (HDX) | theo `last_modified` |
| `sg_nea` (mới) | cụm dịch Singapore (data.gov.sg) | theo ngày |

Bỏ qua: Malaysia iDengue (không có API, chỉ scrape được) và CCHAIN (để sau).

## Các bảng

| Tầng | Bảng | Grain | Ghi chú |
|---|---|---|---|
| Silver | `dengue_cases` | nguồn × địa điểm × kỳ × định nghĩa ca | OpenDengue + WHO, ép kiểu, cột `dq_flags` |
| Silver | `news_articles` | 1 bài báo | dedup theo `guid`, không có thì theo `link`; `countries` = nước của feed ∪ nước được nhắc |
| Silver | `admin_units` | 1 đơn vị cấp tỉnh | SCD2: `valid_from` / `valid_to` |
| Silver | `admin_crosswalk` | cặp mã | ISO 3166-2 ↔ P-code; 63 tỉnh cũ → 34 tỉnh mới của VN |
| Silver | `unit_population` | 1 đơn vị | dân số COD-PS |
| Silver | `province_cases` | nguồn × địa danh × kỳ | OpenDengue cấp tỉnh + TRENDS + PH DOH, đã gắn `unit_id` |
| Silver | `news_unit_mentions` | bài × đơn vị | gazetteer tên tỉnh |
| Gold | `dim_country`, `dim_date`, `dim_admin_unit` | — | star schema; `UNK` cho tin không gắn được nước |
| Gold | `bridge_map_polygon` | đơn vị ↔ polygon | nối đơn vị với hình Natural Earth để vẽ bản đồ |
| Gold | `fact_monthly_cases` | nước × tháng × nguồn | mỗi tháng chọn đúng 1 chuỗi: month > week, isoweek > epiweek, Total > … |
| Gold | `fact_unit_monthly_cases` | đơn vị hiện hành × tháng × nguồn | biến thể tên cùng một đơn vị gốc lấy max; đơn vị cũ khác nhau thì cộng |
| Gold | `fact_daily_news`, `fact_unit_daily_news` | nước (đơn vị) × ngày đăng | |
| Gold | `country_risk` | 1 dòng / nước | tháng gần nhất so với cùng tháng của tối đa 5 năm trước (z-score) + tin 30 ngày |
| Gold | `unit_risk` | 1 dòng / đơn vị hiện hành | số ca gần đây, ca / 100.000 dân, số tin 30 ngày, `signal` |
| Gold | `news_feed` | 1 bài | danh sách tin cho app |

### Cấp tỉnh và sáp nhập tỉnh ở Việt Nam

- **Định nghĩa "tỉnh":** Philippines lấy cấp 2 (cấp 1 là vùng); các nước khác lấy cấp 1.
- **Việt Nam** sáp nhập từ 63 xuống 34 tỉnh từ 01/07/2025:
  - 63 tỉnh cũ lấy từ Natural Earth (mã ISO, tên tiếng Việt), có `valid_to` = 30/06/2025.
  - 34 tỉnh mới lấy từ COD-AB (P-code), có `valid_from` = 01/07/2025.
  - Bảng nối cũ → mới dựng bằng phép điểm-trong-polygon: tâm tỉnh cũ rơi vào polygon tỉnh mới nào thì thuộc tỉnh đó.
  - Số liệu và tin tức nhắc tên tỉnh cũ đều quy về tỉnh mới đang có hiệu lực.
- **Sửa dữ liệu Natural Earth:** bản 10m ghi nhầm tên *vùng* cho 3 tỉnh VN-39, VN-53, VN-66. Code sửa lại thành Đồng Nai, Bắc Kạn, Hưng Yên theo trường `gn_name` của chính file đó (`NE_VN_NAME_FIXES`).
- **Nối số ca vào đơn vị:**
  - TRENDS và COD-PS nối thẳng bằng P-code.
  - OpenDengue nối bằng mã và tên cùng lúc; mã sai thì lùi về nối theo tên.
  - PH DOH nối theo tên.
  - Tên được so bằng `levenshtein` sau khi bỏ dấu và bỏ từ chung ("PROVINCE", "DKI"…), ngưỡng 0,75. Ngưỡng này chọn trên các cặp tên thật.
- **`signal` của `unit_risk`:**
  - "ca bệnh" nếu có số ca trong 24 tháng gần nhất;
  - "tin tức" nếu không có số ca gần đây. Khi đó bản đồ tô theo số tin.

### Mức nguy cơ cấp nước

z ≥ 2 là cao, 1–2 là trung bình, dưới 1 là thấp. Baseline có ít hơn 3 năm thì ghi "không đủ dữ
liệu". Đây là thống kê mô tả, không phải mô hình dự báo. Ba quyết định, dựa trên dữ liệu thật
ngày 29/09/2026:

- **Baseline dùng chuỗi hợp nhất.** Mỗi (nước, năm, tháng) lấy WHO nếu có, không thì lấy
  OpenDengue.
  - Lý do: WHO chỉ có số liệu từ 2024 cho 6/9 nước.
  - Căn cứ: trên 162 tháng mà cả hai nguồn cùng có số liệu, độ lệch trung vị là 0% và 140/162
    tháng lệch không quá 20%.
- **Xử lý độ trễ báo cáo.** Tháng mới nhất giảm hơn 50% so với tháng trước bị coi là chưa báo
  cáo đủ; hệ thống đánh giá tháng ổn định gần nhất và ghi `skipped_recent_months`.
  - Ví dụ Indonesia: tháng 6 có 9.264 ca, tháng 7 có 3.514, tháng 8 chỉ có 67.
- **Baseline có độ lệch chuẩn bằng 0 thì so trực tiếp.** Số ca lớn hơn trung bình là cao,
  không thì thấp.

## Báo cáo chất lượng

Mỗi lần chạy ghi `data/quality/quality_<run_id>.json` và `.md`. Tab "Chất lượng dữ liệu" của app
hiển thị báo cáo mới nhất. Các mức:
- **ERROR:** kết quả Gold không đáng tin.
- **WARN:** vấn đề thật của Bronze hoặc của nguồn.
- **INFO:** số đo để biết.

Danh sách kiểm tra đầy đủ nằm ở `transform/checks.py`.

## Giới hạn đã biết (đo ngày 29/09/2026)

- **Chỉ Thái Lan có số ca cấp tỉnh gần đây** (TRENDS). Các nguồn cấp tỉnh còn lại đều cũ:
  - OpenDengue: VN, PH, KHM, LAO chỉ tới 2010;
  - PH DOH: tới 12/2020.

  Ở các nước này, cấp tỉnh chỉ có tín hiệu tin tức.
- **Tin tức gắn được tới tỉnh còn ít** (khoảng 1/10 số bài). Có ba nguyên nhân:
  - nhiều bài nói chung chung;
  - bài thường nhắc tên huyện hoặc thành phố thay vì tên tỉnh;
  - các trang đa ngôn ngữ (vd Vietnam.vn) xuất hiện trong feed của nước khác.
- **PH DOH ghi riêng các thành phố lớn** (Makati, Baguio…), nên khoảng 27% số dòng không nối được
  vào tỉnh. Muốn nối được cần thêm bảng COD cấp 3.
- **Dân số còn thiếu** cho MMR (18 đơn vị) và PHL cấp 2 (88), vì COD-PS không nối được. VN có đủ
  63 tỉnh cũ; dân số tỉnh mới lấy bằng tổng các tỉnh cũ gộp vào.
- **OpenDengue còn 8 địa danh không nối được:**
  - 4 huyện Brunei và "CENTRAL SINGAPORE": không có COD của BRN và SGP;
  - "HA TAY": nhập vào Hà Nội từ 2008;
  - "BABEL": viết tắt của Bangka-Belitung, độ giống tên quá thấp;
  - "MONGAR": tên một huyện của Bhutan, nhiều khả năng nguồn gán sai nước.
- **Indonesia:** COD-AB 2020 vẫn là 34 tỉnh; số liệu của các tỉnh Papua mới được gộp về tỉnh cũ.
- **Kiểm tra "snapshot lặp"** của Bronze cần ít nhất 2 ngày chạy mới có ý nghĩa.
