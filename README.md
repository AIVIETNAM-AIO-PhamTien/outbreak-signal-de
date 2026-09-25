# OutbreakSignal DE

Hệ thống tiếp nhận và lưu trữ dữ liệu cảnh báo sớm dịch sốt xuất huyết ở Đông Nam Á.
Nhóm Microwave — AIO 2026, Module 4 (Data Sources and Data Ingestion using PySpark).

## Phạm vi hiện tại (MVP)

Theo feedback của TA, scope đã được thu gọn còn:

- **2 nguồn dữ liệu thật** (không phải 4 như outline ban đầu)
- **Batch ingestion → Bronze → Silver** (bỏ Kafka streaming và Gold layer khỏi core scope)

| Nguồn | Loại dữ liệu | Vai trò | Lịch chạy |
|---|---|---|---|
| **OpenDengue** | Số ca bệnh (có cấu trúc) | Số liệu nền, đáng tin cậy | 1 lần/ngày |
| **Google News RSS** | Tin tức (phi cấu trúc) | Tín hiệu sớm, nhanh hơn số liệu chính thức | Mỗi 15–60 phút |

Lý do cần cả hai: OpenDengue chính xác nhưng trễ (phát hành theo version, trễ hàng tháng),
tin tức thì nhanh nhưng không có số liệu. Kết hợp mới ra được "cảnh báo sớm".

## Kiến trúc pipeline

```
[Bước 1: Python + requests]              [Bước 2: PySpark]
   gọi API / tải file  →  data/landing/  →  đọc file  →  data/bronze/ (Parquet)
        (raw nguyên trạng)                              (+ cột metadata)
```

**Vì sao phải tách 2 bước:** Spark KHÔNG gọi được API hay crawl web — Spark chỉ biết đọc
file có sẵn trên đĩa. Nên phải dùng Python thuần tải dữ liệu về trước, rồi Spark mới đọc được.

**Quy tắc Bronze:** giữ nguyên trạng — không đổi tên cột, không lọc, không dedup, không ép
kiểu dữ liệu. Chỉ thêm 3 cột metadata (`_source`, `_ingested_at`, `_source_file`) để truy vết
nguồn gốc. Mọi việc làm sạch để Silver layer (tuần sau) xử lý.

## Cài đặt (Windows)

> ⚠️ **Dùng PowerShell, KHÔNG dùng Git Bash.** Git Bash làm hỏng biến `PATH` khi truyền sang
> Java, khiến Spark báo lỗi `UnsatisfiedLinkError: NativeIO$Windows.access0` rất khó hiểu.
> Cùng một đoạn code chạy lỗi trong Git Bash nhưng chạy tốt trong PowerShell.

### 1. Java 17

Spark 3.5 chỉ chạy ổn định trên Java 8/11/17/21. Máy nào đang có Java mới hơn (VD Java 24)
sẽ gặp lỗi, nên cài thêm JDK 17:

```powershell
winget install Microsoft.OpenJDK.17
```

### 2. Thư viện Python

```powershell
pip install -r requirements.txt
```

### 3. winutils.exe (bắt buộc trên Windows)

Spark ghi file thông qua thư viện Hadoop, mà Hadoop trên Windows cần thêm `winutils.exe` và
`hadoop.dll`. Không có 2 file này thì **đọc được nhưng không ghi được**.

```powershell
$hadoopBin = "$env:USERPROFILE\hadoop\bin"
New-Item -ItemType Directory -Force $hadoopBin | Out-Null
$base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
Invoke-WebRequest "$base/winutils.exe" -OutFile "$hadoopBin\winutils.exe"
Invoke-WebRequest "$base/hadoop.dll"   -OutFile "$hadoopBin\hadoop.dll"
```

Dùng bản **hadoop-3.3.6**. (Bản 3.3.5 cần thêm Visual C++ 2010 Redistributable mới chạy được,
nên tránh. Spark 3.5.3 đi kèm Hadoop 3.3.4 nhưng binary 3.3.6 vẫn tương thích.)

### 4. Thiết lập biến môi trường trước khi chạy

Mỗi lần mở PowerShell mới, chạy:

```powershell
. .\setup_env.ps1
```

Script này chỉ set biến cho cửa sổ terminal hiện tại, không ảnh hưởng cài đặt chung của máy.

## Cách chạy

```powershell
. .\setup_env.ps1          # chỉ cần 1 lần mỗi khi mở terminal mới

python src/run_batch.py --source all          # chạy cả 2 nguồn
python src/run_batch.py --source opendengue   # chạy riêng
python src/run_batch.py --source news
```

Kết quả sẽ nằm ở:

```
data/landing/<nguồn>/<ngày>/     # file raw tải về nguyên trạng
data/bronze/<nguồn>/ingest_date=<ngày>/*.parquet
```

### Kiểm tra dữ liệu đã nạp

```powershell
python src/check_bronze.py
```

## Lên lịch chạy tự động (Windows Task Scheduler)

Tạo 2 task riêng vì 2 nguồn có nhịp cập nhật khác nhau:

| Task | Lệnh | Tần suất |
|---|---|---|
| News | `python src/run_batch.py --source news` | Mỗi 30 phút |
| OpenDengue | `python src/run_batch.py --source opendengue` | Mỗi ngày 1 lần |

Không dùng Airflow cho MVP — cài và học Airflow tốn nhiều thời gian nhưng không thêm giá trị
cho phạm vi hiện tại.

## Cấu trúc thư mục

```
src/
  spark_utils.py         # dùng chung: tạo SparkSession + hàm ghi Bronze
  ingest_opendengue.py   # nguồn 1
  ingest_news_rss.py     # nguồn 2
  run_batch.py           # chạy batch, dùng cho scheduler
  check_bronze.py        # đọc lại Bronze để kiểm tra
spikes/                  # script test nhanh từng nguồn (giai đoạn khảo sát)
docs/progress-notes.md   # ghi chú tiến độ để làm báo cáo
data/landing/            # dữ liệu raw tải về (không commit lên git)
data/bronze/             # Parquet đã nạp   (không commit lên git)
```

## Ghi chú kỹ thuật

- **Dữ liệu nhỏ, sao vẫn dùng Spark?** Dataset hiện tại chỉ vài nghìn dòng — pandas thừa sức
  và còn nhanh hơn. Project dùng PySpark vì đây là mục tiêu học của Module 4, không phải vì
  khối lượng dữ liệu đòi hỏi.
- **OpenDengue không phải live data.** Dữ liệu phát hành theo version (V1.3 ra 27/5/2025,
  chứa số liệu tới tháng 4/2025). Vì vậy chỉ cần kiểm tra bản mới 1 lần/ngày.
- **GDELT DOC API** đã được thử làm nguồn tin tức nhưng bị rate-limit (HTTP 429) từ môi
  trường test. Script vẫn giữ ở `spikes/test_gdelt_news.py` để thử lại từ mạng khác.
