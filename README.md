# Dengue SEA — Bronze Ingestion

Pipeline thu thập dữ liệu sốt xuất huyết (dengue) ở 11 nước Đông Nam Á từ các API công khai, crawl theo batch và nạp vào **Bronze layer** (Delta Lake) bằng PySpark.

Phạm vi hiện tại dừng ở Bronze layer. Mục tiêu dài hạn là dữ liệu đầu vào cho một trang web cảnh báo vùng có nguy cơ bùng phát.

## Nguồn dữ liệu

| Nguồn | Nội dung | Nhịp crawl |
|---|---|---|
| [GDELT DOC 2.0 API](https://api.gdeltproject.org/api/v2/doc/doc) | Tin tức nhắc tới dengue, lọc theo nước | 15 phút |
| [Singapore NEA Dengue Clusters](https://data.gov.sg/datasets/d_dbfabf16158d1b0e1c420627c0819168/view) | Cụm dịch: số ca + polygon toạ độ | 30 phút |
| [WHO GHO dengue](https://xmart-api-public.who.int/ARBOV/V_DENGUE_GLOBAL_VALIDATED_PUBLIC) | Số ca theo quốc gia | 60 phút |

Lưu ý: GDELT **không** cung cấp số ca — chỉ đếm bài báo, dùng làm tín hiệu sớm. Ngoài Singapore, các nước khác hiện chỉ có số ca ở mức quốc gia với tần suất thô (WHO).

---

## Yêu cầu

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Java | OpenJDK **17** | PySpark cần JRE kể cả khi chạy local mode |
| Python | **3.12.x** | Xem mục [Lưu ý về phiên bản](#lưu-ý-về-phiên-bản) |
| pyspark | **≥ 3.5.9** | Bản cũ hơn crash trên Windows — xem mục Lưu ý |
| winutils | chỉ **Windows** | Xem bước 2 |

Kiểm tra Java:

```bash
java -version          # phải ra 17.x
echo $JAVA_HOME        # Linux/macOS
```
```powershell
echo $env:JAVA_HOME    # Windows
```

Nếu chưa có:
- Windows: `winget install Microsoft.OpenJDK.17`
- Ubuntu/Debian: `sudo apt install openjdk-17-jdk`
- macOS: `brew install openjdk@17`

---

## Cài đặt

### Bước 1 — Virtual environment (mọi hệ điều hành)

Dùng [uv](https://docs.astral.sh/uv/) (khuyến nghị — tự tải đúng bản Python, không phụ thuộc Python có sẵn trên máy):

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv -r requirements.txt
```

Hoặc dùng `venv` chuẩn nếu máy đã có Python 3.12:

```bash
# Linux / macOS
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
```
```powershell
# Windows
py -3.12 -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

### Bước 2 — winutils (CHỈ Windows)

> **Linux / macOS: bỏ qua hoàn toàn bước này.** Nhảy thẳng xuống Bước 3.

Spark trên Windows cần `winutils.exe` + `hadoop.dll` để thao tác quyền file — **kể cả khi chỉ ghi ra ổ đĩa local**. Thiếu chúng sẽ gặp:

```
java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
```

Hai file này tồn tại vì Windows không có tương đương POSIX cho `chmod`/`chown`. Trên Linux/macOS, Hadoop gọi thẳng syscall của hệ điều hành nên không cần gì cả.

**Cách lấy:**

1. Vào https://github.com/cdarlint/winutils
2. Chọn thư mục khớp nhánh Hadoop mà Spark đang dùng — hiện dùng **`hadoop-3.3.6`** (xem cách kiểm chứng bên dưới)
3. Tải `bin/winutils.exe` và `bin/hadoop.dll`
4. Đặt vào `.hadoop/bin/` trong repo:

```
.hadoop/
└── bin/
    ├── winutils.exe
    └── hadoop.dll
```

Không cần set `HADOOP_HOME` thủ công — `ingestion/common/spark_session.py` tự trỏ vào `.hadoop/` khi khởi tạo SparkSession.

**Kiểm chứng phiên bản cần dùng:**

```powershell
Get-ChildItem .venv\Lib\site-packages\pyspark\jars -Filter "hadoop-client-api*.jar"
# hadoop-client-api-3.3.4.jar  → dùng winutils nhánh 3.3.x
```

> ⚠️ `cdarlint` hiện chỉ có `hadoop-3.3.5` và `hadoop-3.3.6`, **không có đúng 3.3.4**. Bản 3.3.6 cùng nhánh minor nên thực tế chạy được, nhưng đây là bản gần nhất chứ không phải khớp chính xác — nếu gặp lỗi lạ liên quan quyền file thì thử `hadoop-3.3.5`.

**Vì sao là repo này mà không phải link nào khác?**

Apache **chưa** phát hành bản build Windows chính thức. [HADOOP-18135](https://issues.apache.org/jira/browse/HADOOP-18135) (*"Produce Windows binaries of Hadoop"*) đã được Resolved/Fixed nhưng nhắm tới **Hadoop 3.5.0** — bản chưa ra (mới nhất hiện tại là 3.4.3, phát hành 24/02/2026). Nên hiện vẫn phải lấy từ bên thứ ba.

Chuỗi tin cậy của `cdarlint`: repo [steveloughran/winutils](https://github.com/steveloughran/winutils) — tác giả là **committer của Apache Hadoop** (username `stevel`), ký GPG các bản build bằng khoá công bố trên [ASF committer keylist](https://people.apache.org/keys/committer/stevel) — ghi rõ trong README của mình rằng `cdarlint/winutils` là nơi kế nhiệm và khuyên người dùng sang đó lấy bản mới. Đây không phải chứng thực mật mã cho từng file của `cdarlint`, nhưng là mức bảo chứng cao nhất hiện có.

> `.hadoop/` được `.gitignore` loại khỏi repo có chủ đích: đây là file nhị phân do bên thứ ba build, không nên phân phối lại kèm source code.

### Bước 3 — Kiểm tra cài đặt

```bash
# Linux / macOS
PYTHONPATH=$(pwd) .venv/bin/python scripts/smoke_test.py
```
```powershell
# Windows
$env:PYTHONPATH = (Get-Location).Path
.venv\Scripts\python.exe scripts\smoke_test.py
```

Thành công khi thấy bảng 3 dòng `ok`, dòng `Smoke test OK`, và exit code 0.

---

## Cấu trúc thư mục

```
.
├── CLAUDE.md                  # bối cảnh dự án + quy ước làm việc
├── requirements.txt
├── ingestion/
│   ├── common/
│   │   └── spark_session.py   # builder SparkSession + Delta, dùng chung mọi job
│   ├── sg_nea_dengue.py       # Singapore NEA
│   ├── gdelt_news.py          # GDELT
│   └── who_gho_dengue.py      # WHO GHO
├── scheduler/
│   └── run_scheduler.py       # APScheduler, đăng ký 3 job
├── scripts/
│   └── smoke_test.py
├── tests/
├── data/                      # gitignored — tự sinh khi chạy
│   ├── bronze/<nguồn>/        # Delta table mỗi nguồn
│   └── _control/              # watermark mỗi nguồn
└── .hadoop/bin/               # gitignored, chỉ Windows — xem Bước 2
```

---

## Lưu ý về phiên bản

**`pyspark` phải ≥ 3.5.9.** Các bản 3.5.x trước đó dính [SPARK-53759](https://issues.apache.org/jira/browse/SPARK-53759): `createDataFrame()` làm Python worker crash với lỗi `Python worker exited unexpectedly (crashed)`. Bug này **chỉ xảy ra trên Windows** + Python 3.12/3.13 + local mode; Linux/macOS không bị. Đã vá ở 3.5.9 / 4.0.3 / 4.1.2.

**Python 3.12 chạy được nhưng không nằm trong danh sách hỗ trợ chính thức.** Metadata của pyspark 3.5.9 chỉ khai báo tới Python 3.11 (`Classifier: Programming Language :: Python :: 3.11`), dù `Requires-Python: >=3.8` vẫn cho cài. Project này chạy 3.12.13 và hoạt động bình thường, nhưng nếu gặp lỗi lạ ở tầng PySpark thì đây là nghi phạm đầu tiên — cách loại trừ là dựng venv Python 3.11 để so sánh.

## Cảnh báo vô hại trong log

**Windows** — cuối mỗi job thường có:
```
ERROR ShutdownHookManager: Exception while deleting Spark temp dir: ... antlr4-runtime.jar
java.io.IOException: Failed to delete: ...
```
Do Windows còn giữ lock file `.jar` lúc JVM tắt. Không ảnh hưởng dữ liệu — cứ nhìn exit code.

**Linux / macOS** — đầu mỗi job thường có:
```
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
using builtin-java classes where applicable
```
Hadoop có thư viện native tuỳ chọn (`libhadoop.so`) để tối ưu nén/checksum. Không có thì Spark dùng bản Java thuần, chạy bình thường.
