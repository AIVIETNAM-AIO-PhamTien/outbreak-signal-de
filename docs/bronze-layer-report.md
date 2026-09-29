# Báo cáo: Tầng Bronze — OutbreakSignal DE

Tóm tắt kỹ thuật việc chọn nguồn, thu thập, và lưu trữ dữ liệu dengue Đông Nam Á vào tầng
Bronze. Mọi con số trong tài liệu này đều lấy từ lần chạy thật ngày 2026-09-29, không phải
ước tính.

---

## 1. Chọn nguồn dữ liệu

### 1.1 Quy trình chọn

Outline ban đầu nhắm 4 nguồn (OpenDengue, WHO, ProMED, HealthMap). Sau feedback TA, scope
MVP thu gọn còn 2 nguồn thật + batch ingestion → Bronze → Silver. Trước khi viết ingestion
chính thức, mỗi nguồn đều được **spike test** riêng (script trong `spikes/`) để xác nhận:
API/file gọi được, schema có đủ trường cần, và ước lượng khối lượng dữ liệu thật — tránh
thiết kế xong mới phát hiện dữ liệu không như kỳ vọng.

### 1.2 Bốn nguồn đang ingest

| Nguồn | Loại | Vai trò | Độ trễ | Độ phủ SEA |
|---|---|---|---|---|
| **OpenDengue** (`Spatial_extract`) | Ground truth, cấp quốc gia + tỉnh | Số liệu nền, lịch sử dài | ~17 tháng (đợt phát hành gần nhất tới 04/2025) | 11/11 nước |
| **Google News RSS** | Tin tức, phi cấu trúc | Tín hiệu sớm | Gần real-time | Suy luận, không đảm bảo |
| **WHO GHO** (xMart OData) | Ground truth, cấp quốc gia | Kiểm chứng chéo với OpenDengue | ~5 tuần (mới nhất 24/08/2026) | 9/11 nước |
| **Singapore NEA** | Sự kiện, cấp cụm phố | Bằng chứng khái niệm độ chi tiết cao | 1–4 ngày | 1/11 nước |

**Nguồn đã khảo sát và không triển khai:**

- **GDELT** — `HTTP 429` xác nhận lại 3 lần (kể cả giãn 20s, xin 5 bản ghi/1 ngày) — chặn ở
  mức mạng, không phải do gọi dồn dập. Giữ `spikes/test_gdelt_news.py` để thử lại từ mạng
  cá nhân.
- **ProMED, HealthMap** — không có cơ chế truy cập công khai hợp lệ trong thời gian dự án.
  Không scrape endpoint không được công bố.

> ⚠️ **Lưu ý về scope:** 4 nguồn hiện tại vượt quá "2 nguồn MVP" TA đã duyệt. WHO GHO và
> Singapore NEA được thêm vì lấp đúng lỗ hổng thật (độ trễ, độ chi tiết địa lý) — nhưng đây
> là thay đổi cần báo lại TA trước khi coi là chính thức, chưa làm tại thời điểm viết báo
> cáo này.

---

## 2. Python dùng để làm gì

Python thuần (không qua Spark) đảm nhiệm toàn bộ việc **giao tiếp với nguồn** — vì Spark
không tự gọi API hay crawl web được, chỉ đọc file có sẵn trên đĩa. Cụ thể:

| Việc | Thư viện | Ở đâu |
|---|---|---|
| Gọi HTTP GET tới API/file | `requests` | `fetch_raw()` mỗi module |
| Giải nén file zip | `zipfile` (thư viện chuẩn) | `ingestion/opendengue.py` |
| Parse XML → dict | `xml.etree.ElementTree` (thư viện chuẩn) | `ingestion/news_rss.py` |
| Gọi API 2 chặng (poll-download → S3 presigned URL) | `requests` + retry (`tenacity`) | `ingestion/sg_nea_dengue.py` |
| Dựng câu truy vấn OData (`$filter=ISO3 in (...)`) | thuần Python string | `ingestion/who_gho.py` |
| Ghi file raw xuống đĩa, tính checksum SHA-256 | thư viện chuẩn (`pathlib`, `hashlib`) | `ingestion/common/metadata.py` |
| Đọc/parse config, sinh đường dẫn, log | `PyYAML`, `logging` | `ingestion/common/*.py` |

Ranh giới rõ ràng: **Python dừng lại ở việc ghi file xuống `data/landing/`**. Mọi thứ sau đó
(đọc file, xử lý, ghi Bronze) là việc của PySpark.

---

## 3. PySpark dùng để làm gì

PySpark bắt đầu **sau khi** file đã nằm trên đĩa (`data/landing/`), đảm nhiệm toàn bộ phần
biến file thành bảng có cấu trúc:

| Việc | Hàm/API | Ở đâu |
|---|---|---|
| Đọc CSV/JSON từ landing thành DataFrame | `spark.read.csv()`, `spark.read.json()` | `load_to_bronze()` mỗi module |
| Đọc GeoJSON đã flatten sẵn (Python) | `spark.createDataFrame()` + schema tường minh | `ingestion/sg_nea_dengue.py` |
| Kiểm tra dữ liệu đọc được, đếm dòng | `.count()`, `.columns` | `ingestion/common/validation.py` |
| Lọc phạm vi SEA (chỉ OpenDengue) | `.filter(F.col("adm_0_name").isin(...))` | `ingestion/opendengue.py` |
| Thêm cột lineage | `.withColumn()` | `ingestion/common/bronze.py` |
| Ghi ra Delta Lake, đảm bảo idempotency | `.write.format("delta").option("replaceWhere", ...)` | `ingestion/common/bronze.py` |

**Vì sao dùng Spark dù dữ liệu không lớn:** khối lượng hiện tại (70K–1 dòng mỗi bảng) pandas
xử lý dư sức và nhanh hơn. Dùng PySpark vì đây là mục tiêu học của môn học (Module 4), không
phải vì khối lượng dữ liệu đòi hỏi — điểm này nên ghi thẳng vào báo cáo cuối kỳ vì nó cho
thấy nhóm hiểu khi nào thật sự cần Spark chứ không dùng cho có.

---

## 4. Landing data lưu như nào

**Vai trò:** bản sao **y nguyên byte-for-byte** những gì nguồn trả về — source of truth,
không bao giờ bị sửa sau khi ghi.

**Cấu trúc:** `data/landing/<nguồn>/<ngày>/<file>`

| Nguồn | File lưu ở landing | Vì sao 2 file (nếu có) |
|---|---|---|
| `opendengue` | `.zip` + `.csv` đã giải nén | Chỉ cần CSV để Spark đọc; zip giữ để đối chiếu |
| `news_rss` | `.xml` (gốc) + `.jsonl` (Spark đọc được) | Spark không đọc XML nếu không cài `spark-xml`; jsonl chỉ đổi *cấu trúc chứa*, không sửa nội dung |
| `who_gho` | `.json` (envelope gốc) + `.jsonl` (Spark đọc được) | Cùng lý do: tách mảng `"value"` khỏi envelope OData |
| `sg_nea` | `.json` (GeoJSON gốc) | Python flatten trực tiếp thành rows trước khi đưa vào Spark |

**Cơ chế đặt tên file quyết định idempotency** — đây là điểm quan trọng nhất của landing:

- `opendengue`: tên file **cố định** (`Spatial_extract_V1_3.zip`) — nếu đã tải thì dùng lại,
  không tải lại (dataset chỉ đổi vài tháng một lần).
- `news_rss`: tên file theo **run_id** (mỗi lần fetch một file riêng) — **cố ý**, vì mỗi
  fetch là một quan sát mới thật sự, cần cộng dồn trong ngày.
- `who_gho`, `sg_nea`: tên file theo **ngày** (không phải run_id) — vì đây là nguồn
  snapshot ground-truth, gọi lại trong ngày trả về gần như cùng dữ liệu. Ban đầu `who_gho`
  bị đặt tên theo run_id giống `news_rss`, gây lỗi thật: chạy 3 lần trong ngày ra 9 dòng
  thay vì 3 — đã sửa, xem `docs/progress-notes.md` (29/9).

---

## 5. Bronze lưu data như nào

**Nguyên tắc:** giữ nguyên trạng dữ liệu nguồn — không đổi tên cột, không chuẩn hoá giá
trị, không ép kiểu, không xoá trùng (trừ một ngoại lệ có ghi chú ở mục 5.3).

**Định dạng:** Delta Lake, phân vùng theo `ingestion_date`. Đường dẫn:
`data/bronze/<nguồn>/ingestion_date=<ngày>/`

### 5.1 Bốn cột lineage — duy nhất được phép thêm

| Cột | Ý nghĩa |
|---|---|
| `_source` | Tên nguồn |
| `_ingested_at` | Thời điểm nạp (ISO-8601 UTC) |
| `_source_file` | File raw trong landing mà dòng này đến từ đó |
| `ingestion_date` | Cột phân vùng |

### 5.2 Bốn bảng hiện có (số liệu thật, 2026-09-29)

| Bảng | Số dòng | Số cột | Cột gốc tiêu biểu |
|---|---|---|---|
| `opendengue` | 70.557 | 20 | `adm_0_name`, `adm_1_name`, `adm_2_name`, `calendar_start_date`, `dengue_total`, `S_res`, `T_res` |
| `news_rss` | 131 | 9 | `title`, `link`, `pubDate`, `source`, `description` |
| `who_gho` | 1.232 | 20 | `COUNTRY`, `ISO3`, `START_DATE`, `CASES`, `WHO_REGION`, `DATE_TYPE`, `SERO_1..4` |
| `sg_nea` | 22 | 13 | `locality`, `case_count`, `polygon_geojson`, `raw_payload` |

### 5.3 Ngoại lệ duy nhất: lọc phạm vi SEA cho OpenDengue

`opendengue` dùng `Spatial_extract` (2.821.799 dòng toàn cầu) thay vì `National_extract` để
có dữ liệu cấp tỉnh — bắt buộc vì mục tiêu dự án là "khoanh vùng nguy cơ" và Silver không
thể suy ngược cấp tỉnh từ dữ liệu đã gộp cấp quốc gia. Nhưng SEA chỉ chiếm 2,5% dữ liệu gốc,
nên **lọc theo 11 nước SEA ngay trước khi ghi Bronze** (landing vẫn giữ 100% file gốc). Đây
là quyết định *phạm vi thu thập*, không phải *biến đổi giá trị* — không đổi tên, không
chuẩn hoá gì trong các dòng còn lại. Chi tiết: `README.md` mục 4.1.

`who_gho` lọc theo cùng logic nhưng **ở tầng request** (`$filter=ISO3 in (...)` của OData) vì
API hỗ trợ lọc server-side thật — sạch hơn, không cần tải dữ liệu ngoài phạm vi về máy rồi
mới bỏ.

### 5.4 Idempotency

Mỗi lần chạy, phân vùng `ingestion_date` của ngày đó được **dựng lại đầy đủ** từ toàn bộ
file trong landing của ngày, rồi ghi đè bằng `replaceWhere`. Đã verify thật: chạy
`opendengue` và `who_gho` liên tiếp 2 lần trên dữ liệu live, số dòng không đổi
(70.557 → 70.557, 1.232 → 1.232).

### 5.5 Metadata — mỗi lần chạy, thành công hay thất bại

```json
{
  "source": "opendengue",
  "source_url": "https://.../Spatial_extract_V1_3.zip",
  "ingestion_date": "2026-09-29",
  "status": "success",
  "record_count": 70557,
  "raw_files": [{"bytes": 54687820, "sha256": "69f6f798..."}],
  "duration_seconds": 33.9
}
```

Lưu tại `data/metadata/<nguồn>/ingestion_date=<ngày>/<run_id>.json` — khác với 4 cột lineage
trong Bronze (lineage ở mức *từng dòng*, metadata mô tả *cả lần chạy*).

---

## 6. Mô tả dữ liệu (EDA)

EDA đầy đủ, chạy được (Google Colab, không cần cài gì trên máy) nằm ở
[`notebooks/eda_colab_bronze.ipynb`](../notebooks/eda_colab_bronze.ipynb) — phủ
`opendengue`, `news_rss`, `sg_nea`. Phần dưới là tóm tắt các phát hiện chính, cộng thêm
`who_gho` (chưa có trong notebook, verify trực tiếp qua Spark cho báo cáo này).

### 6.1 OpenDengue — có cấp tỉnh thật, nhưng độ phân giải thời gian đổi theo thời gian

- 70.557 dòng sau lọc SEA: `Admin1` (tỉnh) = 56.656, `Admin2` (huyện) = 9.060, `Admin0`
  (quốc gia) = 4.841 — **329 tỉnh phân biệt, 66 huyện phân biệt**, đủ 11/11 nước.
- `dengue_total` sạch: 0 null, 0 chuỗi rỗng, 0 giá trị âm trên toàn bộ 70.557 dòng.
- **Cột `T_res` (Week/Month/Year) không đè lên nhau** trong cùng một (nước, năm) — đã kiểm
  tra 4.333 cặp (nước, năm), tất cả chỉ có đúng 1 loại `T_res`, nên `SUM()` không bị đếm
  trùng. Nhưng **độ phân giải đổi theo thời gian trong cùng một nước**: Việt Nam chỉ có
  `Year` giai đoạn 1960–1994, `Month` giai đoạn ~1995–2015, `Week` chỉ từ 2016. Không dựng
  được chuỗi thời gian đồng nhất cho toàn bộ lịch sử.
- Bug đã sửa: nguồn ghi `"VIET NAM"` có dấu cách; code lọc cũ dùng `"VIETNAM"` viết liền
  khiến Việt Nam bị loại âm thầm khỏi tập SEA.

### 6.2 Google News RSS — tín hiệu sớm nhưng không có địa điểm

- Không có trường quốc gia/địa điểm nào trong dữ liệu gốc.
- Suy quốc gia từ tiêu đề bằng keyword matching: chỉ **22% bài (15/69) dò được quốc gia** —
  đây là giới hạn trên của độ chính xác nguồn này, không phải lỗi code.
- 131 dòng từ 2 lần fetch, chỉ 66 link phân biệt — **64/131 dòng (49%) trùng lặp theo
  `link`** giữa các lần fetch. Đây là hành vi đúng thiết kế Bronze (mỗi fetch là một quan
  sát riêng); dedup là việc của Silver.

### 6.3 WHO GHO — mới hơn OpenDengue rất nhiều, nhưng nhiều trường phụ gần như trống

- 1.232 dòng, 9/11 nước SEA (thiếu Philippines, Brunei — đã xác nhận không phải lỗi truy
  vấn, nguồn thực sự không có dữ liệu).
- `CASES` sạch 100% (0 null) — là trường duy nhất luôn có giá trị và nên dùng làm chỉ số
  chính. Các trường còn lại **null rất nhiều**, đo trên toàn bộ 1.232 dòng:

  | Cột | % null |
  |---|---|
  | `POPULATION` | 100,0% |
  | `SEVERE_CASES` | 97,2% |
  | `SERO_1`–`SERO_4` | 92,9% mỗi cột |
  | `CONFIRMED_CASES` | 70,2% |
  | `DEATHS` | 28,4% |

- Giống hệt vấn đề `T_res` của OpenDengue: cột `DATE_TYPE` có 3 loại (`month`=534,
  `isoweek`=520, `epiweek`=178 dòng) — độ phân giải thời gian không đồng nhất, cần chọn
  hoặc chuẩn hoá ở Silver.
- Phát hiện dữ liệu bất thường: `MYS` có dòng ngày `2029-06-03`, `SGP` có dòng
  `2028-06-04` — **ngày trong tương lai**, rõ ràng lỗi từ phía nguồn WHO, không phải lỗi
  ingest. Bronze giữ nguyên (đúng nguyên tắc), Silver cần lọc/gắn cờ.

### 6.4 Singapore NEA — phân bố lệch cực mạnh

- 22 dòng (2 batch), phân bố số ca lệch mạnh: giá trị lớn nhất áp đảo hoàn toàn các cụm
  còn lại — **trung bình không mô tả đúng cụm nào**, nên dùng trung vị hoặc giá trị lớn
  nhất cho mọi chỉ số rủi ro sau này.

### 6.5 Phát hiện quan trọng nhất: ba nguồn không cùng cấp địa lý

| Nguồn | Đơn vị địa lý |
|---|---|
| `opendengue` | Quốc gia hoặc tỉnh (`VIET NAM`, `AN GIANG`) |
| `who_gho` | Quốc gia (`ISO3`) |
| `news_rss` | **Không có** — suy luận, 22% recall |
| `sg_nea` | Cụm phố, chuỗi tự do (`Bt Batok St 21 (Blk 207...)`) |

Không có khoá chung để join trực tiếp cả 4 nguồn. Đây là quyết định thiết kế lớn nhất của
tầng Silver — chi tiết và hai phương án khả dĩ đã bàn trong notebook EDA, mục "Ghép ba
nguồn: vấn đề thật sự".

---

## 7. Chưa làm / còn thiếu

- WHO GHO chưa có mặt trong notebook EDA chính thức (mục 6.3 ở trên verify riêng cho báo
  cáo này, chưa gộp vào `eda_colab_bronze.ipynb`).
- `news_rss`, `sg_nea` chưa có integration test tự động (source → Bronze → metadata) —
  `opendengue` và `who_gho` đã có.
- Chưa báo TA về việc dùng 4 nguồn thay vì 2.
- Chưa có tầng Silver — 0 dòng code, đúng phạm vi hiện tại của báo cáo này.
