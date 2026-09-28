# OutbreakSignal DE — Dengue Early Warning (Bronze Ingestion)

Hệ thống thu thập và lưu trữ dữ liệu cảnh báo sớm dịch sốt xuất huyết ở Đông Nam Á.
Nhóm Microwave — AIO 2026, Module 4 (*Data Sources and Data Ingestion using PySpark*).

## 1. Mục tiêu

Phát hiện sớm các khu vực có nguy cơ bùng phát sốt xuất huyết ở Đông Nam Á.

Phạm vi hiện tại dừng ở **tầng Bronze**: thu thập dữ liệu từ nhiều nguồn khác nhau, giữ
nguyên trạng, và đưa vào một data lake có cấu trúc để các bước sau xử lý tiếp.

Theo feedback của TA, scope MVP đã được thu gọn còn **2 nguồn thật**, luồng chính là
**batch ingestion → Bronze → Silver**. Kafka streaming và Gold layer chuyển thành extension.

## 2. Nguồn dữ liệu

| Nguồn | Nội dung | Định dạng | Lịch chạy | Vai trò |
|---|---|---|---|---|
| **[OpenDengue](https://opendengue.org/data.html)** | Số ca bệnh theo quốc gia, cả SEA | CSV trong zip | 1 lần/ngày | Số liệu nền, đáng tin cậy |
| **[Google News RSS](https://news.google.com/rss)** | Tin tức nhắc tới dengue | XML | Mỗi 30 phút | Tín hiệu sớm, nhanh hơn số liệu chính thức |
| [Singapore NEA](https://data.gov.sg/datasets/d_dbfabf16158d1b0e1c420627c0819168/view) | Cụm dịch: số ca **+ polygon toạ độ** | GeoJSON | 1 lần/ngày | Nguồn bổ sung, đã hoàn thiện |

Lý do cần cả hai nguồn MVP: OpenDengue chính xác nhưng trễ (phát hành theo version, trễ
hàng tháng), tin tức thì nhanh nhưng không có số liệu. Kết hợp mới ra được "cảnh báo sớm".

Singapore NEA **không nằm trong 2 nguồn MVP** nhưng được giữ lại vì code đã hoàn thiện và
đây là nguồn **duy nhất có toạ độ thật** — thứ mà bài toán "khoanh vùng nguy cơ" cần.

Nguồn đã khảo sát nhưng **chưa triển khai** — xem mục [11](#11-hạn-chế-đã-biết-về-truy-cập-nguồn).

## 3. Kiến trúc

```mermaid
flowchart LR
    subgraph acq["Thu thập (Python thuần + requests)"]
        OD[OpenDengue<br/>zip → CSV]
        RSS[Google News RSS<br/>XML]
        NEA[Singapore NEA<br/>GeoJSON]
    end

    subgraph landing["data/landing/ — bản gốc nguyên trạng"]
        L[(file raw<br/>theo nguồn + ngày)]
    end

    subgraph spark["PySpark"]
        V[Validation<br/>ingestion-level]
        W[Ghi Delta<br/>+ cột lineage]
    end

    subgraph bronze["data/bronze/ — Delta, phân vùng theo ingestion_date"]
        B[(opendengue)]
        B2[(news_rss)]
        B3[(sg_nea)]
    end

    M[(data/metadata/<br/>1 JSON mỗi lần chạy)]

    OD --> L
    RSS --> L
    NEA --> L
    L --> V --> W
    W --> B & B2 & B3
    W -.-> M
    acq -.-> M
```

**Vì sao tách 2 bước:** Spark **không** gọi được API hay crawl web — Spark chỉ biết đọc file
có sẵn trên đĩa. Nên phải dùng Python thuần (`requests`) tải dữ liệu về `data/landing/`
trước, rồi Spark mới đọc được. Logic thu thập của từng nguồn tách hẳn khỏi phần xử lý Spark.

## 4. Nguyên tắc tầng Bronze

Bronze **giữ nguyên trạng dữ liệu nguồn**. Không đổi tên cột, không lọc, không xoá trùng,
không ép kiểu dữ liệu, không join, không suy ra trường mới.

Ví dụ — nguồn trả về:

```json
{"country": "VNM", "reporting_period": "2026-06", "confirmed_cases": 120}
```

Bronze phải giữ **y nguyên** như vậy. Việc đổi `"VNM"` → `"Vietnam"`, đổi tên
`confirmed_cases` → `cases`, hay parse `"2026-06"` thành ngày, đều là việc của **Silver**.

Thứ duy nhất Bronze được phép thêm là 4 cột truy vết:

| Cột | Ý nghĩa |
|---|---|
| `_source` | Tên nguồn |
| `_ingested_at` | Thời điểm nạp (ISO-8601 UTC) |
| `_source_file` | File raw trong landing mà dòng này đến từ đó |
| `ingestion_date` | Cột phân vùng, dạng `YYYY-MM-DD` |

**Hai bản dữ liệu, hai vai trò:**

- `data/landing/` — bản **gốc byte-for-byte**, không bao giờ bị sửa. Đây là source of truth.
- `data/bronze/` — bản **Delta** để Spark query được, giữ nguyên nội dung + 4 cột trên.

Tách hai cái vì Bronze cần query được, mà nguyên tắc "giữ nguyên trạng nguồn" cũng phải có
chỗ để thoả mãn.

## 5. Cấu trúc thư mục

```
.
├── configs/
│   └── sources.yaml              # mọi endpoint / lịch chạy / bật-tắt nguồn
├── ingestion/
│   ├── common/
│   │   ├── spark_session.py      # SparkSession + Delta, dùng chung mọi job
│   │   ├── config.py             # đọc sources.yaml
│   │   ├── paths.py              # landing / bronze / metadata + mốc thời gian
│   │   ├── logging.py            # log ra console + logs/ingestion_<ngày>.log
│   │   ├── metadata.py           # bản ghi metadata mỗi lần chạy
│   │   ├── validation.py         # kiểm tra mức ingestion
│   │   └── bronze.py             # ghi Delta + cột lineage
│   ├── opendengue.py             # nguồn MVP 1
│   ├── news_rss.py               # nguồn MVP 2
│   └── sg_nea_dengue.py          # nguồn bổ sung
├── scripts/
│   ├── run_batch.py              # chạy batch — điểm vào chính
│   ├── check_bronze.py           # đọc lại Bronze để kiểm tra
│   ├── eda_bronze_sg_nea.py      # EDA bảng sg_nea
│   └── smoke_test.py             # kiểm tra Spark + Delta + Java
├── spikes/                       # script test nhanh từng nguồn (giai đoạn khảo sát)
├── notebooks/
├── tests/
├── docs/
│   ├── progress-notes.md         # ghi chú tiến độ, dùng viết report
│   └── data-profile-sg-nea.md    # hồ sơ dữ liệu nguồn NEA
├── data/                         # gitignored — tự sinh khi chạy
│   ├── landing/<nguồn>/<ngày>/
│   ├── bronze/<nguồn>/ingestion_date=<ngày>/
│   └── metadata/<nguồn>/ingestion_date=<ngày>/*.json
├── logs/                         # gitignored
└── .hadoop/bin/                  # gitignored, chỉ Windows — xem Bước 3
```

## 6. Cài đặt

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Java | OpenJDK **17** | PySpark cần JRE kể cả khi chạy local mode |
| Python | **3.11** hoặc 3.12 | xem [Lưu ý về phiên bản](#lưu-ý-về-phiên-bản) |
| pyspark | **3.5.9** | bản cũ hơn crash trên Windows — xem Lưu ý |
| winutils | chỉ **Windows** | xem Bước 3 |

> ⚠️ **Trên Windows: dùng PowerShell, KHÔNG dùng Git Bash.** Git Bash trộn lẫn dấu gạch
> xuôi/ngược khi truyền `PATH` sang JVM, khiến Spark báo `UnsatisfiedLinkError:
> NativeIO$Windows.access0` rất khó hiểu. Cùng một đoạn code chạy lỗi trong Git Bash nhưng
> chạy tốt trong PowerShell.

### Bước 1 — Java 17

```powershell
java -version          # phải ra 17.x
winget install Microsoft.OpenJDK.17   # nếu chưa có (Windows)
```
```bash
sudo apt install openjdk-17-jdk       # Ubuntu/Debian
brew install openjdk@17               # macOS
```

### Bước 2 — Virtual environment

```powershell
# Windows
py -3.11 -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```
```bash
# Linux / macOS
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Bước 3 — winutils (CHỈ Windows)

> **Linux / macOS: bỏ qua bước này.**

Spark trên Windows cần `winutils.exe` + `hadoop.dll` để thao tác quyền file — **kể cả khi
chỉ ghi ra ổ đĩa local**. Thiếu chúng sẽ gặp `HADOOP_HOME and hadoop.home.dir are unset`.
Hai file này tồn tại vì Windows không có tương đương POSIX cho `chmod`/`chown`.

```powershell
New-Item -ItemType Directory -Force .hadoop\bin | Out-Null
$base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
Invoke-WebRequest "$base/winutils.exe" -OutFile ".hadoop\bin\winutils.exe"
Invoke-WebRequest "$base/hadoop.dll"   -OutFile ".hadoop\bin\hadoop.dll"
```

Không cần set `HADOOP_HOME` thủ công — `ingestion/common/spark_session.py` tự trỏ vào
`.hadoop/` khi khởi tạo SparkSession.

Dùng bản **hadoop-3.3.6**. Bản 3.3.5 đòi thêm Visual C++ 2010 Redistributable
(`exitCode=-1073741515`) nên tránh. Spark 3.5 đi kèm Hadoop 3.3.4 nhưng `cdarlint` không có
đúng 3.3.4; 3.3.6 cùng nhánh minor nên chạy được.

> `.hadoop/` bị gitignore có chủ đích: đây là binary do bên thứ ba build, không nên phân
> phối lại kèm source code. Apache chưa phát hành bản build Windows chính thức
> ([HADOOP-18135](https://issues.apache.org/jira/browse/HADOOP-18135) nhắm tới Hadoop 3.5.0,
> chưa ra).

### Bước 4 — Kiểm tra cài đặt

```powershell
$env:PYTHONPATH = (Get-Location).Path
.venv\Scripts\python.exe scripts\smoke_test.py
```
```bash
PYTHONPATH=$(pwd) .venv/bin/python scripts/smoke_test.py
```

Thành công khi thấy bảng 3 dòng `ok` và dòng `Smoke test OK`.

## 7. Chạy từng nguồn

```powershell
$env:PYTHONPATH = (Get-Location).Path

.venv\Scripts\python.exe scripts\run_batch.py --source opendengue
.venv\Scripts\python.exe scripts\run_batch.py --source news_rss
.venv\Scripts\python.exe scripts\run_batch.py --source sg_nea
```

Chạy nhiều nguồn trong một lệnh: lặp lại `--source`.

Muốn tắt hẳn một nguồn thì sửa `enabled: false` trong `configs/sources.yaml`, **không sửa code**.

## 8. Chạy toàn bộ

```powershell
.venv\Scripts\python.exe scripts\run_batch.py
```

Kết quả (chạy thật ngày 2026-09-28):

```
============================================================
TONG KET INGESTION
============================================================
  opendengue  SUCCESS  29,873 dong
  news_rss    SUCCESS  130 dong
  sg_nea      SUCCESS  22 dong
  who_gho     SKIPPED  Chua trien khai. Ngoai scope MVP theo feedback TA.
  gdelt       SKIPPED  Bi rate-limit (HTTP 429) tu mang test...
============================================================
```

**Mỗi nguồn chạy độc lập.** Một nguồn chết không kéo theo nguồn khác, và dữ liệu nguồn đã
nạp thành công **không bị xoá**. Exit code là 1 nếu có nguồn `FAILED` (`SKIPPED` không tính
là lỗi).

### Lên lịch tự động (Windows Task Scheduler)

| Task | Lệnh | Tần suất |
|---|---|---|
| News | `run_batch.py --source news_rss` | 30 phút |
| OpenDengue | `run_batch.py --source opendengue` | 1 lần/ngày |
| SG NEA | `run_batch.py --source sg_nea` | 1 lần/ngày, khung 15–16h SGT |

Không dùng Airflow cho MVP — cài và học tốn nhiều thời gian nhưng không thêm giá trị cho
phạm vi hiện tại.

## 9. Kết quả Bronze mong đợi

```powershell
.venv\Scripts\python.exe scripts\check_bronze.py
```

| Nguồn | Số dòng | Số cột | Cột gốc được giữ nguyên |
|---|---|---|---|
| opendengue | 29.873 | 20 | `adm_0_name`, `ISO_A0`, `calendar_start_date`, `dengue_total`, … |
| news_rss | 65 / lần fetch | 9 | `title`, `link`, `pubDate`, `source`, `description` |
| sg_nea | ~11 cụm / snapshot | 13 | `locality`, `case_count`, `polygon_geojson`, `raw_payload`, … |

Số cột = cột gốc + 4 cột truy vết ở [mục 4](#4-nguyên-tắc-tầng-bronze).

### Idempotency — chọn cách ghi đè phân vùng ngày

Mỗi lần chạy, phân vùng của ngày được **dựng lại đầy đủ** từ toàn bộ file raw trong landing
của ngày đó, rồi ghi đè bằng `replaceWhere`. Các ngày khác không bị động đến.

Hệ quả: chạy lại bước nạp bao nhiêu lần cũng ra cùng kết quả, **không bao giờ nối thêm bản
sao**. Riêng nguồn tin tức và NEA, mỗi lần *fetch mới* là một quan sát riêng nên số dòng
tăng — đó là chủ ý, không phải lỗi trùng lặp. Việc gộp bài trùng là của Silver.

## 10. Cấu trúc metadata

Mỗi lần chạy — **thành công hay thất bại** — đều để lại một file
`data/metadata/<nguồn>/ingestion_date=<ngày>/<run_id>.json`:

```json
{
  "source": "opendengue",
  "source_type": "file_download",
  "source_format": "csv",
  "source_url": "https://github.com/OpenDengue/master-repo/raw/main/...",
  "ingestion_date": "2026-09-28",
  "ingestion_timestamp": "2026-09-28T11:15:18.472913+00:00",
  "run_id": "20260928T111518Z",
  "ingestion_mode": "batch",
  "status": "success",
  "record_count": 29873,
  "raw_files": [{"path": "...", "bytes": 316140, "sha256": "..."}],
  "bytes_downloaded": 316140,
  "source_version": "V1.3",
  "duration_seconds": 32.94,
  "error_message": null
}
```

Metadata mô tả **thao tác ingestion**, không phải schema nghiệp vụ của dữ liệu. Nó khác với
các cột `_source` / `_ingested_at` / `_source_file` trong bảng Bronze — đó là lineage ở mức
**từng dòng**. Cần cả hai.

## 11. Hạn chế đã biết về truy cập nguồn

| Nguồn | Trạng thái | Chi tiết |
|---|---|---|
| **GDELT DOC 2.0** | Bị chặn | Trả HTTP 429 liên tục từ mạng test dù đã giảm tần suất. Nghi do IP dùng chung bị chặn sẵn chứ script viết đúng docs. Giữ `spikes/test_gdelt_news.py` để thử lại từ mạng khác. Đã thay bằng Google News RSS. |
| **WHO GHO** | Chưa triển khai | Endpoint đã xác định, nhưng ngoài scope MVP 2 nguồn theo feedback TA. Khai báo sẵn trong `configs/sources.yaml` với `enabled: false`. |
| **ProMED** | Không làm | Không có cơ chế truy cập công khai phù hợp trong thời gian còn lại. Không triển khai để tránh scrape endpoint không được phép. |
| **HealthMap** | Không làm | Như trên. Không tạo implementation giả, không bypass authentication, không scrape API không công bố. |
| **Google News RSS** | Hạn chế nội dung | Không có trường quốc gia hay địa điểm nào. Suy ra quốc gia từ tiêu đề là việc của Silver (keyword matching), và sẽ có sai số — cần nói rõ trong report. |
| **OpenDengue** | Không phải live data | Phát hành theo version (V1.3 ra 27/05/2025, số liệu chỉ tới 04/2025). Kiểm tra bản mới 1 lần/ngày là đủ. |
| **Singapore NEA** | Poll quá dày | NEA publish quanh 15:00 SGT, nội dung chỉ đổi mỗi 1–4 ngày. Poll 60 phút/lần nghĩa là ~99% số dòng là lặp lại — xem `docs/data-profile-sg-nea.md`. |

## 12. Chạy test

```powershell
$env:PYTHONPATH = (Get-Location).Path
.venv\Scripts\python.exe -m pytest                        # toàn bộ
.venv\Scripts\python.exe -m pytest -m "not integration"    # bỏ qua test cần JVM
```

| Nhóm test | Nội dung |
|---|---|
| `test_config.py` | Đọc YAML, trộn defaults, bật/tắt nguồn, báo lỗi rõ ràng |
| `test_paths.py` | Sinh đường dẫn, mốc thời gian, tách thư mục theo nguồn |
| `test_metadata.py` | Đủ trường bắt buộc, checksum, duration, lần chạy thất bại |
| `test_validation.py` | Nguồn không với tới được, file rỗng, file thiếu, landing rỗng |
| `test_sg_nea_ingest.py` | Các hàm thuần của nguồn NEA |
| `test_integration_bronze.py` | **nguồn → ingestion → Bronze → metadata**, chạy thật Spark + Delta |

## 13. Ngoài phạm vi hiện tại

Chưa triển khai, và **không** thêm vào nếu không có yêu cầu rõ ràng:

Silver layer · Gold layer · schema chung · chuẩn hoá / làm sạch dữ liệu · dedup · NLP ·
trích xuất thực thể · feature engineering · machine learning · dự đoán bùng phát · chấm
điểm rủi ro · dashboard · Kafka · streaming · Snowflake · dbt · Dagster · Airflow · LLM ·
vector database · data warehouse.

Silver là bước kế tiếp theo feedback TA, nhưng **chưa có dòng code nào** trong repo này.

## Lưu ý về phiên bản

**`pyspark` phải là 3.5.9.** Các bản 3.5.x trước đó dính
[SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759): `createDataFrame()` làm
Python worker crash. Bug này **chỉ xảy ra trên Windows** + Python 3.12/3.13 + local mode.

**Cảnh báo vô hại trong log:**

- *Windows* — cuối mỗi job: `ERROR ShutdownHookManager: Exception while deleting Spark temp
  dir: ... antlr4-runtime.jar`. Do Windows còn giữ lock file `.jar` lúc JVM tắt. Không ảnh
  hưởng dữ liệu — cứ nhìn exit code và bảng tổng kết.
- *Linux / macOS* — đầu mỗi job: `WARN NativeCodeLoader: Unable to load native-hadoop
  library`. Hadoop có thư viện native tuỳ chọn; không có thì Spark dùng bản Java thuần.
