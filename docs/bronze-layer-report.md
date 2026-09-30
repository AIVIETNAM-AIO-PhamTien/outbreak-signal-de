# Báo cáo: Tầng Bronze - Project OutbreakSignal DE

Tóm tắt kỹ thuật việc chọn nguồn, thu thập, và lưu trữ dữ liệu dengue Đông Nam Á vào tầng
Bronze.

---

## 1. Chọn nguồn dữ liệu

### 1.1 Quy trình chọn

Outline nhắm 4 nguồn (OpenDengue, WHO, Google News RSS, Singapore NEA). Scope MVP thu gọn còn source 3 nguồn 
chính và 1 nguồn phụ + batch ingestion → Bronze → Silver. Trước khi viết ingestion
chính thức, mỗi nguồn đều được **spike test** riêng (script trong `spikes/`) để xác nhận:
API/file gọi được, schema có đủ trường cần, và ước lượng khối lượng dữ liệu thật.

### 1.2 Ba nguồn đang ingest

| Nguồn | Loại | Vai trò | Độ trễ | Độ phủ SEA |
|---|---|---|---|---|
| **OpenDengue** (`Spatial_extract`) | Ground truth, cấp quốc gia + tỉnh | Số liệu nền, lịch sử dài | ~17 tháng (đợt phát hành gần nhất tới 04/2025) | 11/11 nước |
| **Google News RSS** | Tin tức, phi cấu trúc | Tín hiệu sớm | Gần real-time | Suy luận, không đảm bảo |
| **WHO GHO** (xMart OData) | Ground truth, cấp quốc gia | Kiểm chứng chéo với OpenDengue | ~5 tuần (mới nhất 24/08/2026) | 9/11 nước |

### 1.3 Vì sao đúng 3 nguồn này cho early-warning

Xét đúng mục tiêu "phát hiện sớm cho các nước SEA" (số nhiều): OpenDengue và WHO GHO là
cặp ground truth bổ sung nhau (OpenDengue phủ đủ 11/11 nước nhưng trễ ~17 tháng; WHO GHO
mới hơn nhiều — trễ ~5 tuần — nhưng thiếu Philippines/Brunei). Google News RSS là nguồn
**duy nhất** có nhịp gần real-time — bỏ nguồn này thì hệ thống chỉ còn là thống kê dịch tễ
lịch sử, mất hẳn chữ "sớm" — dù chất lượng tín hiệu hiện còn yếu (suy luận quốc gia chỉ
~22-23%, cần cải thiện bằng NER/geocoding trước khi dùng làm input mô hình thay vì keyword
matching).

**Singapore NEA đã gỡ khỏi pipeline.** Từng có ingestion hoàn chỉnh (chất lượng dữ liệu tốt
nhất trong các nguồn đã thử — toạ độ thật, cập nhật 1–4 ngày) nhưng chỉ phủ 1/11 nước — không
scale được sang 10 nước SEA còn lại nên không thể làm nền cho mô hình chung toàn vùng. Giữ lại
3 nguồn OpenDengue + WHO GHO + Google News RSS vì phủ đủ/gần đủ 11 nước SEA và ổn định nhất
trong các nguồn đã thử. Lịch sử triển khai NEA: xem git log.

---

## 2. Kỹ thuật ingestion

<p align="center"><img src="../assets/outbreak-pipeline.png" alt="Luồng ingest Landing → Bronze" width="600"></p>

*Hình: luồng ingest từ nguồn → Python → `data/landing/` → PySpark → `data/bronze/`. Hình vẽ cả Singapore NEA; nguồn này đã gỡ khỏi pipeline hiện tại.*

Dùng **batch**: mỗi lần chạy là một tiến trình Python độc lập, khởi tạo `SparkSession`
mới, xử lý xong rồi thoát hẳn.

| Nguồn | Lịch chạy | `schedule` trong `sources.yaml` |
|---|---|---|
| `opendengue` | 1 lần/ngày | `daily` |
| `who_gho` | 1 lần/ngày | `daily` |
| `news_rss` | mỗi 30 phút | `every_30_minutes` |

Idempotency (chống trùng lặp dữ liệu) nhờ `replaceWhere` theo `ingestion_date` (chi tiết ở mục 6.4) nên chạy lại
nhiều lần trong ngày không nhân đôi dữ liệu. Hiện tất cả các lần chạy đều thủ công
(`python scripts/run_batch.py`) - Windows Task Scheduler chưa được cấu hình thật, xem
mục 8.

---

## 3. Python dùng để làm gì

Python thuần (không qua Spark) đảm nhiệm toàn bộ việc **giao tiếp với nguồn**, vì Spark
không tự gọi API hay crawl web được, chỉ đọc file có sẵn trên đĩa. Cụ thể:

| Việc | Thư viện | Ở đâu |
|---|---|---|
| Gọi HTTP GET tới API/file | `requests` | `fetch_raw()` mỗi module |
| Giải nén file zip | `zipfile` (thư viện chuẩn) | `ingestion/opendengue.py` |
| Parse XML → dict | `xml.etree.ElementTree` (thư viện chuẩn) | `ingestion/news_rss.py` |
| Dựng câu truy vấn OData (`$filter=ISO3 in (...)`) | thuần Python string | `ingestion/who_gho.py` |
| Ghi file raw xuống đĩa, tính checksum SHA-256 | thư viện chuẩn (`pathlib`, `hashlib`) | `ingestion/common/metadata.py` |
| Đọc/parse config, sinh đường dẫn, log | `PyYAML`, `logging` | `ingestion/common/*.py` |

Ranh giới rõ ràng: **Python dừng lại ở việc ghi file xuống `data/landing/`**. Mọi thứ sau đó
(đọc file, xử lý, ghi Bronze) là việc của PySpark.

---

## 4. PySpark dùng để làm gì

PySpark bắt đầu **sau khi** file đã nằm trên đĩa (`data/landing/`), đảm nhiệm toàn bộ phần
biến file thành bảng có cấu trúc:

| Việc | Hàm/API | Ở đâu |
|---|---|---|
| Đọc CSV/JSON từ landing thành DataFrame | `spark.read.csv()`, `spark.read.json()` | `load_to_bronze()` mỗi module |
| Kiểm tra dữ liệu đọc được, đếm dòng | `.count()`, `.columns` | `ingestion/common/validation.py` |
| Lọc phạm vi SEA (chỉ OpenDengue) | `.filter(F.col("adm_0_name").isin(...))` | `ingestion/opendengue.py` |
| Thêm cột lineage | `.withColumn()` | `ingestion/common/bronze.py` |
| Ghi ra Delta Lake, đảm bảo idempotency | `.write.format("delta").option("replaceWhere", ...)` | `ingestion/common/bronze.py` |

**Vì sao dùng Spark dù dữ liệu không lớn:** khối lượng hiện tại (70K–1 dòng mỗi bảng) pandas
xử lý dư sức và nhanh hơn. Dùng PySpark vì đây là mục tiêu học của môn học (Module 4), không
phải vì khối lượng dữ liệu đòi hỏi.

---

## 5. Landing data lưu như nào

**Vai trò:** bản sao **y nguyên byte-for-byte** những gì nguồn trả về - source of truth,
không bao giờ bị sửa sau khi ghi.

**Cấu trúc:** `data/landing/<nguồn>/<ngày>/<file>`

| Nguồn | File lưu ở landing | Vì sao 2 file (nếu có) |
|---|---|---|
| `opendengue` | `.zip` + `.csv` đã giải nén | Chỉ cần CSV để Spark đọc; zip giữ để đối chiếu |
| `news_rss` | `.xml` (gốc) + `.jsonl` (Spark đọc được) | Spark không đọc XML nếu không cài `spark-xml`; jsonl chỉ đổi *cấu trúc chứa*, không sửa nội dung |
| `who_gho` | `.json` (envelope gốc) + `.jsonl` (Spark đọc được) | Cùng lý do: tách mảng `"value"` khỏi envelope OData |


- `opendengue`: tên file **cố định** (`Spatial_extract_V1_3.zip`) — nếu đã tải thì dùng lại,
  không tải lại (dataset chỉ đổi vài tháng một lần).
- `news_rss`: tên file theo **run_id** (mỗi lần fetch một file riêng) — **cố ý**, vì mỗi
  fetch là một quan sát mới thật sự, cần cộng dồn trong ngày.
- `who_gho`: tên file theo **ngày** (không phải run_id) — vì đây là nguồn snapshot
  ground-truth, gọi lại trong ngày trả về gần như cùng dữ liệu. Ban đầu bị đặt tên theo
  run_id giống `news_rss`, gây lỗi thật: chạy 3 lần trong ngày ra 9 dòng thay vì 3 — đã sửa (29/9).

---

## 6. Bronze lưu data như nào

**Nguyên tắc:** giữ nguyên trạng dữ liệu nguồn: không đổi tên cột, không chuẩn hoá giá
trị, không ép kiểu, không xoá trùng (trừ một ngoại lệ có ghi chú ở mục 6.3).

**Định dạng:** Delta Lake, phân vùng theo `ingestion_date`. Đường dẫn:
`data/bronze/<nguồn>/ingestion_date=<ngày>/`

### 6.1 Bốn cột lineage

| Cột | Ý nghĩa |
|---|---|
| `_source` | Tên nguồn |
| `_ingested_at` | Thời điểm nạp (ISO-8601 UTC) |
| `_source_file` | File raw trong landing mà dòng này đến từ đó |
| `ingestion_date` | Cột phân vùng |

### 6.2 Ba bảng hiện có (số liệu thật, 2026-09-29)

| Bảng | Số dòng | Số cột | Cột gốc tiêu biểu |
|---|---|---|---|
| `opendengue` | 70.557 | 20 | `adm_0_name`, `adm_1_name`, `adm_2_name`, `calendar_start_date`, `dengue_total`, `S_res`, `T_res` |
| `news_rss` | 131 | 9 | `title`, `link`, `pubDate`, `source`, `description` |
| `who_gho` | 1.232 | 20 | `COUNTRY`, `ISO3`, `START_DATE`, `CASES`, `WHO_REGION`, `DATE_TYPE`, `SERO_1..4` |

### 6.3 Lọc phạm vi SEA cho OpenDengue

`opendengue` dùng `Spatial_extract` (2.821.799 dòng toàn cầu) thay vì `National_extract` để
có dữ liệu cấp tỉnh. Mục tiêu dự án là "khoanh vùng nguy cơ" và Silver không
thể suy ngược cấp tỉnh từ dữ liệu đã gộp cấp quốc gia. Nhưng SEA chỉ chiếm 2,5% dữ liệu gốc,
nên **lọc theo 11 nước SEA ngay trước khi ghi Bronze** (landing vẫn giữ 100% file gốc). Chi tiết: `README.md` mục 4.1.

`who_gho` lọc theo cùng logic nhưng **ở tầng request** (`$filter=ISO3 in (...)` của OData) vì
API hỗ trợ lọc server-side thật.

### 6.4 Idempotency

Mỗi lần chạy, phân vùng `ingestion_date` của ngày đó được **dựng lại đầy đủ** từ toàn bộ
file trong landing của ngày, rồi ghi đè bằng `replaceWhere`. Đã verify thật: chạy
`opendengue` và `who_gho` liên tiếp 2 lần trên dữ liệu live, số dòng không đổi
(70.557 → 70.557, 1.232 → 1.232).

### 6.5 Metadata

Cho biết trạng thái mỗi lần chạy, thành công hay thất bại

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

Lưu tại `data/metadata/<nguồn>/ingestion_date=<ngày>/<run_id>.json` - khác với 4 cột lineage
trong Bronze (lineage ở mức *từng dòng*, metadata mô tả *cả lần chạy*).

---

## 7. Mô tả dữ liệu (EDA)

EDA đầy đủ, chạy được (Google Colab, không cần cài gì trên máy) nằm ở
[`notebooks/eda_colab_bronze.ipynb`](../notebooks/eda_colab_bronze.ipynb) — phủ
`opendengue`, `news_rss`, `who_gho` (notebook còn giữ phần EDA của `sg_nea` như tư liệu lịch
sử, nguồn này đã gỡ khỏi pipeline chính thức — xem mục 1.3). Đã chạy thật trên Colab
(29/9/2026), 54 cell, 0 lỗi. Phần dưới là tóm tắt các phát hiện chính.

### 7.1 OpenDengue — có cấp tỉnh thật, nhưng độ phân giải thời gian đổi theo thời gian

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

### 7.2 Google News RSS — tín hiệu sớm nhưng không có địa điểm

- Không có trường quốc gia/địa điểm nào trong dữ liệu gốc.
- Suy quốc gia từ tiêu đề bằng keyword matching: chỉ **22% bài (15/69) dò được quốc gia** —
  đây là giới hạn trên của độ chính xác nguồn này, không phải lỗi code.
- 131 dòng từ 2 lần fetch, chỉ 66 link phân biệt — **64/131 dòng (49%) trùng lặp theo
  `link`** giữa các lần fetch. Đây là hành vi đúng thiết kế Bronze (mỗi fetch là một quan
  sát riêng); dedup là việc của Silver.

### 7.3 WHO GHO — mới hơn OpenDengue rất nhiều, nhưng nhiều trường phụ gần như trống

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
- Kiểm tra trùng lặp theo khoá `(ISO3, START_DATE, DATE_TYPE)`: **35/1.232 dòng trùng khoá**
  — nghĩa là bộ 3 cột này **không phải khoá duy nhất** của bảng, chắc còn một chiều dữ liệu
  khác chưa được profile (ví dụ nhóm tuổi, giới tính, biến thể chỉ số). Cần tìm ra cột phân
  biệt còn thiếu trước khi dùng bộ 3 cột này làm khoá join ở Silver.

### 7.4 Phát hiện quan trọng nhất: 3 nguồn không cùng cấp địa lý

| Nguồn | Đơn vị địa lý |
|---|---|
| `opendengue` | Quốc gia hoặc tỉnh (`VIET NAM`, `AN GIANG`) |
| `who_gho` | Quốc gia (`ISO3`) |
| `news_rss` | **Không có** — suy luận, 22% recall |

Không có khoá chung để join trực tiếp cả 3 nguồn. Đây là quyết định thiết kế lớn nhất của
tầng Silver — chi tiết đã bàn trong notebook EDA, mục "Ghép ba nguồn: vấn đề thật sự".

---

## 8. Chưa làm / còn thiếu

- `news_rss` chưa có integration test tự động (source → Bronze → metadata) —
  `opendengue` và `who_gho` đã có.
- Chưa có tầng Silver.
- Windows Task Scheduler chưa được cấu hình thật — mọi lần chạy hiện vẫn thủ công
  (`python scripts/run_batch.py`), xem mục 2.
