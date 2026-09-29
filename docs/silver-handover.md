# Bàn giao tầng Silver (và Gold) — OutbreakSignal

Tài liệu cho người tiếp quản Silver/Gold. Nội dung gồm ba phần:
- pipeline chạy theo thứ tự nào;
- mỗi bước quyết định gì và vì sao;
- những gì đã phát hiện trên dữ liệu thật và chỗ còn dở.

Danh sách bảng và cột xem [silver-gold-healthmap.md](silver-gold-healthmap.md). Lỗi Bronze đã
sửa xem [bronze-fixes.md](bronze-fixes.md).

Code nằm ở nhánh `feat/silver-gold`, chưa commit:
- `transform/`: logic;
- `scripts/run_transform.py`: điều phối;
- `app/`: HealthMap;
- `tests/`: 162 test.

## 1. Chạy và kiểm tra

```powershell
.venv\Scripts\python.exe scripts\run_batch.py                     # Bronze
.venv\Scripts\python.exe scripts\run_transform.py                 # Silver + Gold (~6 phút)
.venv\Scripts\python.exe scripts\run_transform.py --layer gold    # chỉ Gold (đọc Silver có sẵn)
.venv\Scripts\python.exe -m pytest -q                             # ~5 phút
.venv\Scripts\streamlit.exe run app\healthmap.py                  # http://localhost:8501
```

- **Việc đầu tiên sau mỗi lần chạy:** mở `data/quality/quality_<run_id>.md`. Exit code 1 nghĩa là
  có ERROR, và khi đó Gold không đáng tin.
- **Quy ước code:**
  - Transform là hàm thuần `DataFrame → DataFrame`. Chỉ `run_transform.py` đọc và ghi.
  - Silver/Gold ghi đè toàn bảng mỗi lần chạy, nên chạy lại cho cùng kết quả.
  - Mọi transform viết bằng **PySpark, không dùng Python UDF**.
  - Mọi hàm đều có docstring và type hint.

## 2. Các bước Silver theo thứ tự chạy

### Bước 0 — Kiểm tra Bronze trước khi dùng (`transform/checks.py`)

Kiểm tra theo **hợp đồng schema**:
- thiếu cột là ERROR;
- đổi kiểu hoặc có cột lạ là WARN.

Ngoài ra còn kiểm tra partition mới nhất không rỗng, snapshot lặp (với nguồn theo ngày), và
chạm `$top` của WHO. Bắt lỗi ngay ở cửa vào tốt hơn là để lỗi lọt xuống Gold.

### Bước 1 — `dengue_cases` (`transform/silver/cases.py`)

- **Đầu vào:**
  - OpenDengue: release có version cao nhất (`parse_release`), không lấy theo ngày;
  - WHO: partition ngày mới nhất.

  Mỗi partition đã là một snapshot đầy đủ.
- **Ép kiểu:** Bronze toàn string, Silver cast sang số và ngày. Cast lỗi thì gắn cờ, không bỏ
  dòng một cách lặng lẽ.
- **Quy về một lược đồ chung:**
  - `period_type` gồm week, month, year;
  - `week_system` gồm isoweek, epiweek;
  - `case_definition`.
  - Philippines lấy `adm_2` làm tỉnh.
- **Cột `dq_flags` (mảng):** `future_period`, `start_date_derived`, `confirmed_gt_cases`,
  `irregular_period`… Gold tự quyết loại cờ nào.
- **Dedup theo khoá Silver:** gặp trùng thì giữ dòng có số ca lớn nhất. Không dùng `UUID` của
  OpenDengue làm khoá.

### Bước 2 — `news_articles` (`transform/silver/news.py`)

- **Đầu vào:** đọc **mọi partition** `news_rss`, vì một bài xuất hiện qua nhiều lần fetch.
- **Dedup:** theo `guid`; dòng cũ chưa có `guid` thì lùi về `link`.
- **`first_seen_at` / `last_seen_at`:** lấy `_fetched_at` trước, không có thì lấy run_id trong
  tên file. `_ingested_at` không dùng được vì bị ghi đè.
- **`countries`:** là hợp của nước theo feed (`feed_country`) và nước được nhắc trong
  tiêu đề/mô tả (từ khoá ở `transform/reference.py`).
- **`is_recent`:** bài đăng không quá 7 ngày trước lần thấy đầu.

### Bước 3 — `admin_units` và `admin_crosswalk` (`transform/silver/admin.py`)

- **"Tỉnh" nghĩa là gì:** PHL lấy cấp 2, các nước khác lấy cấp 1 (`PROVINCE_LEVEL`).
- **Đơn vị hiện hành:** lấy từ COD-AB (P-code).
- **Việt Nam sáp nhập 63 → 34 tỉnh (01/07/2025), lưu theo SCD2:**
  - tỉnh cũ lấy từ Natural Earth, `valid_to` = 30/06/2025;
  - tỉnh mới lấy từ COD, `valid_from` = 01/07/2025.
- **Bảng nối mã** dựng bằng phép điểm-trong-polygon viết bằng PySpark (`transform/geo.py`,
  ray casting):
  - tâm đơn vị COD rơi vào polygon Natural Earth nào → cặp (P-code, mã ISO);
  - tâm tỉnh cũ của VN rơi vào polygon tỉnh mới nào → cặp cũ → mới.
- **Sửa lỗi Natural Earth** (`NE_VN_NAME_FIXES`): 3 tỉnh VN bị ghi nhầm thành tên vùng.

### Bước 4 — `unit_population` (`transform/silver/province.py`)

- **Nối bằng P-code** cho THA, IDN, KHM, LAO, MYS, TLS.
- **Nối theo tên** cho VN (COD-PS là 63 tỉnh cũ) và PHL (P-code giữa AB và PS lệch nhau).
- Chỉ lấy dòng tổng: Gender = "all", Age_range = "all".

### Bước 5 — `province_cases` (`transform/silver/province.py`)

- **Nguồn:** OpenDengue cấp tỉnh, TRENDS (Thái Lan) và PH DOH, gắn vào `unit_id`, rồi quy về
  `current_unit_id` là đơn vị có hiệu lực hôm nay.
- **Cách nối:**
  - TRENDS nối thẳng bằng P-code.
  - OpenDengue nối bằng mã và tên cùng lúc; mã sai thì lùi về tên (`match_method`).
  - PH DOH nối theo tên.
- **So tên** bằng `transform/names.py`: bỏ dấu bằng `translate`, bỏ từ chung (PROVINCE, DKI…),
  so bằng `levenshtein`, ngưỡng 0,75. Ngưỡng chọn trên cặp tên thật; test ghi lại cả các cặp
  **không được** khớp.
- VN: tên tỉnh cũ chỉ so với **tỉnh cũ** (`_units_before_reform`), để tránh nhầm "Đồng Nai cũ"
  với "Đồng Nai mới".

### Bước 6 — `news_unit_mentions` (`transform/silver/province.py`)

- **Gazetteer:** tìm tên tỉnh (tên Latin và tên bản xứ) trong tiêu đề và mô tả, chỉ trong các
  nước của bài.
  - Chữ Latin dùng regex có biên từ: `(?<!\p{L})…(?!\p{L})`, để "Albay" không khớp
    "Albayrak".
  - Chữ Thái không có dấu cách giữa các từ nên dùng khớp chuỗi con.
  - Bỏ tên ngắn hơn 4 ký tự.
- **Tên tỉnh cũ của VN** vẫn được tìm (báo chí còn dùng) rồi quy về tỉnh mới.

## 3. Gold, tóm tắt các quy tắc dễ sai

Chi tiết xem [silver-gold-healthmap.md](silver-gold-healthmap.md).

- **Không cộng trùng chuỗi:** mỗi (nguồn, địa điểm, tháng) chọn đúng 1 chuỗi
  (`facts.monthly_series`).
- **Không cộng trùng ở cấp tỉnh:** biến thể tên của cùng một đơn vị gốc thì lấy **max**; các tỉnh
  cũ khác nhau gộp vào một tỉnh mới thì **cộng**.
- **Baseline cấp nước:** dùng chuỗi hợp nhất, ưu tiên WHO, không có thì OpenDengue. Hai nguồn lệch
  trung vị 0% trên 162 tháng trùng nhau.
- **Độ trễ báo cáo:** tháng mới nhất giảm hơn 50% bị bỏ qua.
- **Cấp tỉnh:** số ca cũ hơn 24 tháng thì `case_level` = "số liệu cũ" và `signal` = "tin tức".

## 4. Phát hiện chính trên dữ liệu thật (29–30/09/2026)

| Chủ đề | Phát hiện |
|---|---|
| Số ca cấp nước | WHO có 9/11 nước, trễ khoảng 5 tuần. PHL và BRN chỉ có OpenDengue (tới 2023 và 2017) |
| Nguy cơ hiện tại | KHM (z = 6,45) và MYS (z = 2,54) ở mức "cao"; MMR "không đủ dữ liệu" (2 năm baseline) |
| Số ca cấp tỉnh | **Chỉ Thái Lan có số ca gần đây** (77/77 tỉnh). IDN tới 07/2024, PH DOH tới 2020, VN/KHM/LAO tới 2010 |
| Tin tức | 456 bài; 89% gắn được nước; **chỉ 49 bài (~11%) gắn được tỉnh** |
| Nối số ca | TRENDS 100%; PH DOH 73% (thành phố lớn ghi riêng); OpenDengue còn 8 địa danh không nối được (huyện Brunei, Hà Tây, BABEL, MONGAR, CENTRAL SINGAPORE) |
| Dân số | Thiếu MMR (18 đơn vị) và PHL cấp 2 (88) |
| Bản đồ | 1 đơn vị không vẽ được: City of Isabela (PHL), vì không nằm trong polygon nào |

**Kết luận về độ khả thi:**
- **Cấp nước:** khả thi, có số ca gần đây và baseline.
- **Cấp tỉnh theo số ca:** chỉ khả thi cho Thái Lan.
- **Các nước còn lại ở cấp tỉnh:** chỉ khả thi theo tín hiệu tin tức, và độ phủ hiện còn thấp.

## 5. Bẫy kỹ thuật đã gặp (Windows, PySpark local)

- **`createDataFrame` từ list Python mặc định tạo 20 partition.** Mỗi partition khởi động một
  Python worker, tốn khoảng 16 giây mỗi action; bộ test từng mất 18 phút.
  - Cách xử lý: luôn dùng `transform.common.small_frame()` (1 partition).
  - Python UDF chậm vì cùng lý do, nên đừng thêm.
- **`collect()` đổi timestamp sang giờ máy (UTC+7).** Test nên so chuỗi `date_format`, không so
  đối tượng `datetime`.
- **Một số hàm không nhận Column:** `F.array_position` và `F.instr` chỉ nhận literal. Thay bằng
  `when` hoặc `Column.contains`.
- **Streamlit `cache_data`:** đường dẫn dữ liệu phải là **tham số** của hàm cache, nếu không test
  trỏ sang dữ liệu khác vẫn nhận cache cũ.
- **`AppTest` với selectbox có `format_func`:** `set_value` nhận giá trị gốc (vd `"VNM"`), còn
  `.options` là nhãn đã format.
- **Log `ShutdownHookManager: Failed to delete ... jar` khi tắt JVM:** vô hại, cứ nhìn exit code.
- **Chạy `--layer gold` cũng tạo báo cáo chất lượng**, nhưng chỉ có các check của Gold. App lấy
  file mới nhất nên tab "Chất lượng dữ liệu" mất các WARN của Bronze/Silver cho tới lần chạy đầy
  đủ tiếp theo. Nên sửa để app chọn báo cáo mới nhất *đầy đủ*, hoặc ghi tầng vào tên file.
- **App cache dữ liệu Gold** (`st.cache_data`): chạy lại transform xong phải khởi động lại app.

## 6. Việc còn dở (theo thứ tự nên làm)

1. **Gazetteer cấp huyện/thành phố.**
   - Vấn đề: bài báo hay nhắc tên huyện hoặc thành phố thay vì tên tỉnh.
   - Hướng làm: nạp COD-AB cấp 2 (cấp 3 với PHL), tìm tên đó rồi quy lên tỉnh.
   - Kỳ vọng: nâng tỉ lệ bài gắn được tỉnh, hiện khoảng 11%. Cần cẩn thận với tên trùng giữa các
     tỉnh.
2. **Nối thành phố lớn của PH DOH về tỉnh**, dùng COD-AB cấp 3 của PHL (27% số dòng DOH hiện
   chưa nối được).
3. **Bài đa ngôn ngữ nằm sai feed** (vd Vietnam.vn trong feed nước khác): cân nhắc chỉ tin
   `feed_country` khi ngôn ngữ bài trùng ngôn ngữ feed.
4. **Dân số MMR và PHL cấp 2:** tìm nguồn COD-PS khớp mã.
5. **CCHAIN (Philippines, 12 thành phố):** thêm nếu cần thêm số ca cấp thành phố.
6. **Chạy liên tục nhiều ngày** để kiểm tra snapshot lặp và `first_seen_at` qua nhiều partition.
   Hiện Bronze mới có 1 ngày.
7. **Chốt với nhóm** xem có commit nhánh này không. Nếu có, cần review lại theo working mode của
   dự án.
