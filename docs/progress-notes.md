# Ghi chú tiến độ - OutbreakSignal DE

Note lại sau mỗi buổi làm để cuối kỳ viết report đỡ phải nhớ lại. Mới nhất để trên cùng.

---

## 28/9 - Gộp code cả nhóm về một nhánh

### Vấn đề phải xử lý

Đang có 4 nhánh chạy song song và **không ghép được vào nhau**:

- `main` — SparkSession + Delta, smoke test. File `sg_nea_dengue.py` chỉ có đúng dòng
  `import requests`, README thì mô tả 3 file chưa hề tồn tại.
- `test/check_data_sg-nea` — ingestion NEA hoàn chỉnh, có test, có EDA.
- `opendengue-news-rss` — ingestion OpenDengue + News RSS.
- `data/test-source` — notebook `data_ingestion.ipynb` commit lên **rỗng 0 byte**, chỉ có
  9 file PNG. Người phụ trách cần commit lại phần code.

Nặng nhất là `opendengue-news-rss` là **root commit riêng**, không chung tổ tiên với `main`
(`git merge-base` trả về rỗng). Merge thường sẽ từ chối.

Cách xử lý: `git merge --allow-unrelated-histories` chứ không copy file sang. Copy thì mất
authorship, mà đây là bài tập nhóm nên phần ai làm gì cần giữ lại được trong `git log`.

### Mấy thứ phải chốt lại vì hai nhánh làm khác nhau

| Chuyện | Nhánh này | Nhánh kia | Chốt |
|---|---|---|---|
| Định dạng Bronze | Parquet | Delta | **Delta** — cần `replaceWhere` để chạy lại không nhân đôi |
| Tên cột phân vùng | `ingest_date` | (không có) | **`ingestion_date`** viết đầy đủ |
| SparkSession | `src/spark_utils.py` | `ingestion/common/spark_session.py` | Giữ bản Delta, bê `showConsoleProgress=false` từ bản kia sang |
| pyspark | 3.5.3 | 3.5.9 | **3.5.9** — 3.5.3 dính đúng SPARK-53759 mà README bên kia cảnh báo |
| Bố cục | `src/` phẳng | `ingestion/` | **`ingestion/`** — import được bình thường, không phụ thuộc `sys.path[0]` |

### Ba lớp mới, trước đây chưa nhánh nào có

- `configs/sources.yaml` — mọi endpoint gom về một chỗ. Trước đây URL nằm rải rác thành
  hằng số trong từng file.
- `ingestion/common/metadata.py` — mỗi lần chạy đẻ ra một JSON: lấy từ đâu, bao nhiêu dòng,
  mất bao lâu, lỗi gì. Chạy hỏng cũng vẫn ghi. Trước đây không có gì cả.
- `ingestion/common/validation.py` + `logging.py` — thay cho `print()`.

### Hai lỗi thật sự sửa được trong lúc gộp

**`run_batch.py` cũ gọi job trần, không bọc try/except.** `ingest_opendengue.main()` chỉ bắt
`requests.RequestException`, nên một lỗi `BadZipFile` hay lỗi Spark là vỡ cả vòng lặp —
nguồn thứ hai không chạy, bảng tổng kết không in. Giờ mỗi nguồn bọc riêng.

**NEA không giữ bản raw nào.** Nó ghi thẳng từ bộ nhớ vào Delta. Giờ lưu GeoJSON gốc xuống
`data/landing/` như hai nguồn kia.

### Kết quả chạy thật 28/9

| Nguồn | Số dòng | Số cột |
|---|---|---|
| opendengue | 29.873 | 20 |
| news_rss | 65 / lần fetch | 9 |
| sg_nea | 11 cụm / snapshot | 13 |

Test: 66 pass (60 unit + 6 integration). Integration test chạy thật Spark + Delta, đi hết
chuỗi nguồn → landing → Bronze → metadata, có cả test chạy 3 lần vẫn ra 3 dòng.

### Còn lại

- Notebook rỗng ở nhánh `data/test-source` — cần commit lại.
- Silver layer: chưa có dòng nào. Đây là việc tuần sau theo feedback TA.
- Chưa set Task Scheduler thật, mới chạy tay.

---

## Tuần 2 - làm phần ingestion và Bronze layer

### Mấy thứ phải tự quyết

**Data ít thế này có cần Spark không?**

Thật ra là không cần. Data đang có chỉ hơn 3 nghìn dòng cho Đông Nam Á, mỗi lần lấy tin tức
thêm được 66 bài. Pandas làm thừa sức mà còn nhanh hơn, vì Spark riêng khởi động đã mất mấy
giây rồi.

Nhưng Module 4 là học PySpark nên vẫn dùng Spark. Chỗ này nên viết thẳng vào report, vì nó
cho thấy nhóm hiểu khi nào mới thật sự cần Spark chứ không phải thấy gì cũng bê vào dùng.

**Spark không crawl web được**

Lúc đầu mình hiểu sai chỗ này. Cứ tưởng viết PySpark là nó tự đi gọi API lấy data về. Hoá ra
Spark chỉ đọc được file có sẵn trên đĩa thôi.

Nên pipeline bắt buộc phải tách hai bước: Python dùng requests tải file về `data/landing/`
trước, rồi Spark mới đọc file đó để ghi Parquet ra `data/bronze/`. Hai bước tách bạch hẳn.

Cũng may là mấy script test nguồn tuần trước không phí, nó chính là bước 1 rồi, tuần này chỉ
phải viết thêm bước 2.

**Bronze thì giữ nguyên, không được sửa gì**

Quy tắc tự đặt ra để khỏi phải suy nghĩ nhiều mỗi lần viết code: không đổi tên cột, không lọc,
không xoá trùng, không ép kiểu dữ liệu. Chỉ thêm 3 cột `_source`, `_ingested_at`,
`_source_file` để sau này còn biết dòng đó ở đâu ra.

Nên đọc CSV để `inferSchema=False`, tức là tất cả đều thành string. Cố ý như vậy, chứ không
phải quên. Cũng không lọc riêng Đông Nam Á ở Bronze, giữ đủ cả 29.873 dòng gốc, việc lọc để
Silver làm. Tin tức cũng vậy, không đoán quốc gia từ tiêu đề ở bước này.

**Chia thư mục theo ngày**

Để dạng `data/bronze/<nguồn>/ingest_date=YYYY-MM-DD/`. Mỗi lần chạy nó ghi đè đúng thư mục
của ngày hôm đó nên chạy lại mấy lần cũng không bị nhân đôi data.

Riêng tin tức hơi khác vì một ngày chạy nhiều lần. Cách xử lý là Spark đọc lại toàn bộ file
JSONL trong landing của ngày đó rồi ghi đè, nên vẫn gom đủ các lần chạy mà không trùng.

**Hai nguồn chạy lịch khác nhau**

Tin tức thì 15-60 phút một lần vì bài mới ra liên tục. OpenDengue thì một ngày một lần thôi,
vì họ phát hành theo version, mấy tháng mới có bản mới, chạy dày hơn cũng chỉ tải lại đúng
cái file cũ.

**Không dùng Airflow**

Chỉ dùng Task Scheduler của Windows gọi `src/run_batch.py`. Airflow cài với học mất cả tuần
mà MVP không được thêm gì, để sau nếu còn thời gian.

**Chia file cho dễ chia việc**

Phần Spark khó hiểu gom hết vào `src/spark_utils.py`, cả nhóm đọc một lần là xong. Còn mỗi
nguồn là một file riêng tầm 60 dòng để một người nhận một file. Làm vậy cho khớp với ý TA là
chia owner theo module nhưng vẫn review chéo được.

### Ba cái lỗi lúc chạy Spark trên Windows

Phần này ghi kỹ vì cả 8 người chắc chắn sẽ gặp y hệt, khỏi mất công mò lại.

Lỗi đầu tiên là Java. Máy đang có sẵn Java 24 mà Spark 3.5 chỉ chạy được trên Java 8, 11, 17
hoặc 21. Phải cài thêm JDK 17 bằng `winget install Microsoft.OpenJDK.17` rồi trỏ `JAVA_HOME`
vào đó. Mình có viết sẵn hàm check Java trong `spark_utils.py`, sai là nó in ra hướng dẫn sửa
luôn, khỏi đoán.

Lỗi thứ hai là thiếu `winutils.exe`. Triệu chứng lạ lắm: đọc data thì bình thường mà ghi là
lỗi, báo `HADOOP_HOME and hadoop.home.dir are unset`. Lý do là Spark ghi file thông qua thư
viện Hadoop, mà Hadoop trên Windows bắt buộc phải có `winutils.exe` với `hadoop.dll`. Tải hai
file này từ repo cdarlint/winutils. Lưu ý phải lấy bản hadoop-3.3.6. Mình thử bản 3.3.5 trước
thì nó đòi cài thêm Visual C++ 2010 Redistributable (báo lỗi `exitCode=-1073741515`), còn bản
3.3.6 thì chạy được luôn không cần cài gì thêm.

Lỗi thứ ba mất nhiều thời gian nhất. Đã có đủ winutils rồi mà vẫn lỗi
`UnsatisfiedLinkError: NativeIO$Windows.access0`. Lúc đầu cứ đinh ninh là do lệch phiên bản
Hadoop, vì binary là 3.3.6 mà jar đi kèm Spark là 3.3.4. Tìm mãi không có bản 3.3.4 ở đâu cả.
Hoá ra không phải vậy, nguyên nhân thật là chạy bằng Git Bash. Git Bash nó trộn lẫn dấu gạch
xuôi với gạch ngược khi truyền biến PATH sang tiến trình Java nên JVM không nạp được
`hadoop.dll`. Đổi sang chạy bằng PowerShell là hết, y nguyên đoạn code đó.

Rút ra: **dùng PowerShell, đừng dùng Git Bash**. Mình có viết thêm `setup_env.ps1` để set biến
môi trường bằng một lệnh, cả nhóm khỏi gõ tay sai.

### Kết quả chạy ngày 25/9

| Nguồn | Số dòng vào Bronze | Số cột |
|---|---|---|
| opendengue | 29.873 | 20 |
| news_rss | 132 | 9 |

Cột opendengue là 20 vì file gốc có 16 cột, cộng 3 cột metadata với 1 cột `ingest_date` Spark
tự sinh từ tên thư mục. News thì 5 cột gốc nên ra 9.

Đọc lại kiểm tra bằng `python src/check_bronze.py`, Spark tự nhận ra `ingest_date` là partition
mà không cần khai báo gì. Cột `_source_file` dùng hàm `input_file_name()` nên mỗi dòng biết
chính xác nó đến từ file nào, cái này quan trọng với tin tức vì một ngày fetch mấy lần.

Chỗ 132 dòng tin tức dễ bị hiểu nhầm là lỗi trùng lặp nên note lại: đó là 2 lần fetch, mỗi lần
66 bài. Không phải lỗi. Mỗi lần fetch tính là một lần quan sát riêng, Bronze giữ hết, việc gộp
bài trùng để Silver tuần sau làm. Đúng với quy tắc Bronze giữ nguyên trạng đã đặt ra ở trên.

### Chưa làm xong

- File outline chính thức vẫn chưa sửa theo feedback TA (bỏ Kafka với Gold, chốt lại 2 nguồn,
  sửa nội dung tuần 2-4 cho khớp). Cái này phải làm trước khi nộp.
- Chưa set Task Scheduler thật, mới chỉ chạy tay.
- Data trong `data/landing/` sẽ phình dần, tầm 11MB một ngày nếu chạy đủ lịch. Project 4 tuần
  thì không sao nhưng nếu muốn gọn thì xoá landing của mấy ngày cũ đi, Bronze mới là cái cần
  giữ (có 281KB thôi).

---

## 25/9 - Feedback của TA và test thử các nguồn

### TA nói gì

TA bảo hướng đi ổn, chủ đề hợp và bài toán thực tế rõ ràng, nhưng scope ban đầu to quá so với
thời gian còn lại. Chốt lại mấy điểm:

MVP chỉ làm 2 nguồn thật thôi chứ không phải 3-4 nguồn như outline cũ. Luồng chính là batch
ingestion vào Bronze rồi chuẩn hoá về một schema chung ở Silver.

Phần Kafka giả lập báo cáo ca bệnh real-time thì chuyển thành extension, làm sau nếu còn thời
gian. Gold layer với star schema TA cũng không nhắc tới trong phần mô tả MVP nên coi như cũng
nằm ngoài scope chính, giống Kafka.

Có một ý quan trọng là phải test nhanh từng nguồn trên sample nhỏ trước đã, xác nhận API với
schema có dùng được thật không rồi mới code tiếp. Tránh thiết kế xong mới phát hiện data không
như mình tưởng.

Nhóm 8 người thì chia owner theo module, nhưng phải review chéo để ai cũng hiểu toàn bộ
pipeline. Cái này cần cho lúc viết report cuối, với cả yêu cầu người ngoài nhóm phải chạy lại
được.

Việc cần làm sau buổi này: cập nhật lại outline cho khớp, thêm bảng phân công với lịch review.

### Test thử 3 nguồn

Script để trong thư mục `spikes/`.

**OpenDengue** dùng được. Tải file zip chứa CSV từ GitHub về, tổng 29.873 dòng, lọc riêng 10
nước Đông Nam Á còn 3.262 dòng. Có đủ field cần: địa điểm là mấy cột `adm_0_name`,
`adm_1_name`, `adm_2_name`, thời gian là `calendar_start_date` với `calendar_end_date`, số ca
là `dengue_total`.

**GDELT** thì chưa xác nhận được. Gọi API bị trả về HTTP 429 liên tục, tức là bị chặn vì gọi
quá nhiều, dù đã chờ với giảm tần suất rồi vẫn vậy. Nghi là do IP dùng chung của môi trường
đang test bị chặn sẵn chứ script viết theo đúng docs. Cần ai đó thử lại từ mạng khác xem sao,
chưa loại hẳn.

**Google News RSS** dùng được, lấy làm nguồn tin tức thay cho GDELT. Không cần API key, test
thật ra 66 bài cho query "dengue Southeast Asia". Nhược điểm là không có field quốc gia hay
địa điểm nào cả, phải tự đoán từ tiêu đề ở bước Silver, chắc dùng keyword matching là đủ.

Chốt 2 nguồn cho MVP: OpenDengue lấy số ca, Google News RSS lấy tin tức. GDELT giữ lại làm dự
phòng, trong report ghi rõ là cần test thêm từ mạng khác chứ không nói là không dùng được.

### OpenDengue không phải live data

Chỗ này quan trọng vì ảnh hưởng tới lịch chạy batch.

Nó không phải API trả kết quả real-time mà là dataset tải theo version, từ V1.0 lên V1.2.x rồi
V1.3. Lịch phát hành cũng không đều, có đợt cách nhau mấy tháng (V1.2.0 ra 29/9/2024 mà V1.2.1
mãi 9/5/2025 mới có), có đợt lại cách nhau vài ngày (V1.2.1 ngày 9/5, V1.2.2 ngày 16/5, V1.3
ngày 27/5).

Bản mới nhất cũng bị trễ. V1.3 phát hành 27/5/2025 nhưng số liệu chỉ có tới tháng 4/2025. Xem
trong data thì độ phân giải thời gian chủ yếu là tuần, tháng, năm, đúng kiểu số liệu dịch tễ
tổng hợp định kỳ chứ không phải log sự kiện theo giờ.

Nên cái ý định ban đầu là crawl 15-60 phút một lần chỉ hợp với nguồn tin tức thôi. OpenDengue
thì kiểm tra version mới mỗi ngày là đủ, chạy dày hơn cũng chỉ tải về đúng một file không đổi.

### Làm rõ: OpenDengue là data thật, không phải data giả lập

Đọc lại outline cũ mục V.2 thấy dễ gây hiểu nhầm nên note lại cho rõ. Chỗ đó gộp chung hai
việc khác nhau.

Việc thứ nhất là batch ingest trực tiếp vào Bronze. Đây là data thật, tải thật, nạp thẳng,
không giả lập gì cả.

Việc thứ hai là lấy record thật từ OpenDengue rồi phát lại qua Kafka như thể phòng khám đang
gửi báo cáo real-time. Chỗ này có giả lập, nhưng cái được giả lập là **cơ chế gửi dữ liệu**
chứ không phải nội dung số liệu. Số liệu vẫn là thật, chỉ là phát lại cho giống đang chảy
real-time thôi.

Việc thứ hai đã bị cắt khỏi MVP theo feedback TA rồi. Nên hiện tại OpenDengue chỉ dùng theo
việc thứ nhất, không cần dựng Kafka hay giả lập gì hết.

---

## Tham khảo

- Script test nguồn: `spikes/test_opendengue.py`, `spikes/test_gdelt_news.py`,
  `spikes/test_google_news_rss.py`
- Sample data lưu ở `output/bronze_test/`
- OpenDengue: https://opendengue.org/data.html
- GDELT: https://api.gdeltproject.org/api/v2/doc/doc
