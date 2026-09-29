# Handout: Bàn giao Bronze → Silver — OutbreakSignal DE

Tài liệu này viết cho người tiếp theo bắt đầu tầng **Silver**. Mục đích: hiểu tầng Bronze
đã làm gì mà không cần đọc lại toàn bộ code, và có sẵn bảng mô tả dữ liệu (tên cột, kiểu,
ý nghĩa) của cả 3 bảng Bronze để bắt tay vào việc luôn.

Tài liệu kỹ thuật đầy đủ hơn: [`docs/bronze-layer-report.md`](bronze-layer-report.md) (report
chi tiết) và [`README.md`](../README.md) (hướng dẫn cài đặt, chạy, kiến trúc).

---

## 1. Bronze đã làm gì (tóm tắt 1 phút)

- **Mục tiêu dự án**: phát hiện sớm khu vực có nguy cơ bùng dịch sốt xuất huyết ở Đông Nam Á.
- **3 nguồn đã ingest**, mỗi nguồn 1 bảng Delta riêng, **không join, không merge** ở Bronze:
  `opendengue`, `news_rss`, `who_gho`. (Singapore NEA từng có ingestion hoàn chỉnh nhưng đã
  gỡ khỏi pipeline — chỉ phủ 1/11 nước SEA, không scale cho mục tiêu số nhiều "các nước SEA".
  Lịch sử: xem git log.)
- **Kỹ thuật**: batch ingestion (không phải streaming/micro-batch). Python thuần tải dữ
  liệu về `data/landing/` (bản gốc, y nguyên byte-for-byte), PySpark đọc lại, thêm 4 cột
  lineage, ghi ra Delta ở `data/bronze/<nguồn>/ingestion_date=<ngày>/`.
- **Nguyên tắc Bronze**: giữ nguyên trạng dữ liệu nguồn — không đổi tên cột, không ép kiểu,
  không chuẩn hoá giá trị, không xoá trùng. Tất cả việc làm sạch/chuẩn hoá là việc của Silver.
- **Một ngoại lệ duy nhất**: `opendengue` bị lọc còn 11 nước SEA trước khi ghi Bronze (từ
  2.821.799 dòng toàn cầu xuống 70.557 dòng) — đây là chọn **phạm vi thu thập**, không phải
  biến đổi giá trị. Landing vẫn giữ 100% file gốc chưa lọc. Chi tiết: report mục 6.3.
- **Idempotency**: mỗi lần chạy dựng lại toàn bộ phân vùng `ingestion_date` của ngày đó rồi
  ghi đè bằng `replaceWhere` — chạy lại bao nhiêu lần cũng ra cùng kết quả, không nhân đôi.

## 2. Vai trò từng nguồn cho Silver

Xét đúng mục tiêu "phát hiện sớm cho SEA" (không phải chỉ thống kê lịch sử):

| Nguồn | Vai trò cho Silver | Vì sao |
|---|---|---|
| **OpenDengue** | Ground truth nền | Phủ đủ 11/11 nước, lịch sử dài, nhưng trễ ~17 tháng — dùng để train/backtest, không dùng cảnh báo hiện tại |
| **WHO GHO** | Ground truth đối chiếu | Trễ chỉ ~5 tuần, mới hơn OpenDengue nhiều — dùng đối chiếu/hiệu chỉnh, dù thiếu Philippines + Brunei |
| **Google News RSS** | Tín hiệu sớm | Nguồn **duy nhất** gần real-time — không có nguồn này thì mất chữ "sớm" trong early-warning, dù chất lượng suy luận địa điểm còn yếu (~22-23% recall) |

## 3. Vấn đề đã biết mà Silver cần xử lý

Đây là danh sách quan trọng nhất của tài liệu này — mỗi dòng là một quyết định thiết kế
Silver phải đưa ra, không phải bug cần "sửa":

1. **Không có khoá chung để join 3 nguồn.** `opendengue` dùng tên nước UN (`VIET NAM`),
   `who_gho` dùng `ISO3`, `news_rss` không có trường quốc gia (phải tự suy luận). → Cần bước
   **chuẩn hoá khoá địa lý** (khuyến nghị: map hết về ISO3) trước khi so sánh/join bất cứ gì.
2. **Độ phân giải không gian khác cấp.** OpenDengue có tỉnh/huyện, WHO GHO chỉ quốc gia,
   News RSS không có. So sánh chéo nguồn chỉ làm được ở cấp quốc gia (cấp thấp nhất chung).
3. **Độ phân giải thời gian không đồng nhất, kể cả trong 1 nguồn.** `opendengue.T_res`
   (Week/Month/Year) đổi theo giai đoạn của cùng một nước (VD: Việt Nam chỉ có `Year` giai
   đoạn 1960–1994, `Month` giai đoạn ~1995–2015, `Week` từ 2016). `who_gho.DATE_TYPE` cũng
   có 3 loại (`month`/`isoweek`/`epiweek`) trộn lẫn. → Cần chuẩn hoá về 1 đơn vị thời gian
   chung trước khi ghép chuỗi thời gian.
4. **`who_gho`: khoá `(ISO3, START_DATE, DATE_TYPE)` không phải khoá duy nhất** — 35/1.232
   dòng trùng khoá này (verify 29/9/2026). Có khả năng còn một chiều dữ liệu khác chưa được
   profile (nhóm tuổi, giới tính, biến thể chỉ số...). Cần tìm cột phân biệt còn thiếu
   trước khi dùng bộ 3 cột này làm khoá join.
5. **`who_gho`: 2 dòng có ngày trong tương lai** (`MYS` = `2029-06-03`, `SGP` = `2028-06-04`)
   — lỗi từ phía nguồn WHO, Bronze giữ nguyên theo đúng nguyên tắc. Silver cần lọc/gắn cờ.
6. **`who_gho`: nhiều cột phụ gần như trống** — `POPULATION` 100% null, `SEVERE_CASES`
   97,2% null, `SERO_1..4` 92,9% null mỗi cột, `CONFIRMED_CASES` 70,2% null. Chỉ `CASES`
   sạch 100% — nên dùng làm chỉ số chính, các cột khác chỉ bổ sung khi có.
7. **`news_rss`: không có trường quốc gia/địa điểm nào.** Suy luận bằng keyword matching
   chỉ đạt ~22-23% recall — là giới hạn trên hiện tại, không phải lỗi code. Cần cải thiện
   bằng NER/geocoding thật nếu muốn dùng làm input mô hình.
8. **`news_rss`: mỗi lần fetch là một quan sát riêng, cố ý không dedup ở Bronze** — cùng
   một bài có thể xuất hiện lặp ở nhiều lần fetch (49% trùng theo `link` trong một lô test).
   Dedup theo `link` là việc của Silver.
9. **`news_rss.source` (tên nhà xuất bản, từ tag RSS) dễ nhầm với cột lineage `_source`**
   (tên nguồn ingest, luôn là chuỗi `"news_rss"`) — để ý khi viết code Silver.

## 4. Data dictionary — 3 bảng Bronze

**4 cột lineage giống nhau ở cả 3 bảng** (thêm bởi `ingestion/common/bronze.py`, không có
trong dữ liệu gốc):

| Cột | Kiểu | Mô tả |
|---|---|---|
| `_source` | string | Tên nguồn ingest, cố định theo bảng (`"opendengue"`, `"news_rss"`, `"who_gho"`) |
| `_ingested_at` | string | Thời điểm PySpark ghi dòng này vào Bronze, ISO-8601 UTC |
| `_source_file` | string | Tên file raw trong `data/landing/` mà dòng này đọc ra từ đó |
| `ingestion_date` | string | Cột phân vùng Delta, dạng `YYYY-MM-DD` |

Phần dưới là các cột **gốc từ nguồn** — giữ nguyên tên, chưa qua chuẩn hoá.

### 4.1 `opendengue` (Spatial_extract, đã lọc 11 nước SEA)

| Cột | Kiểu | Mô tả |
|---|---|---|
| `adm_0_name` | string | Tên quốc gia (UN name, viết hoa, VD `"VIET NAM"` có dấu cách) |
| `adm_1_name` | string | Tên tỉnh/bang (Admin cấp 1) |
| `adm_2_name` | string | Tên huyện/quận (Admin cấp 2) |
| `full_name` | string | Tên đầy đủ ghép các cấp hành chính lại với nhau |
| `ISO_A0` | string | Mã ISO Alpha-3 của quốc gia |
| `FAO_GAUL_code` | string | Mã đơn vị hành chính theo FAO GAUL (Global Administrative Unit Layers) |
| `RNE_iso_code` | string | Mã tham chiếu đơn vị hành chính nội bộ của OpenDengue (chưa xác minh kỹ ngữ nghĩa — cần dùng thì kiểm tra thêm tài liệu OpenDengue gốc) |
| `IBGE_code` | string | Mã hành chính IBGE (chỉ áp dụng cho Brazil — với SEA luôn 1 giá trị cố định/rỗng) |
| `calendar_start_date` | string | Ngày bắt đầu kỳ báo cáo |
| `calendar_end_date` | string | Ngày kết thúc kỳ báo cáo |
| `Year` | string | Năm của kỳ báo cáo |
| `dengue_total` | string | Tổng số ca dengue trong kỳ + khu vực này (kiểu string — Bronze không ép kiểu, cần cast khi qua Silver) |
| `case_definition_standardised` | string | Định nghĩa ca bệnh đã chuẩn hoá do nước báo cáo dùng (quan sát được 3 loại) |
| `S_res` | string | Độ phân giải không gian của dòng: `Admin0` (quốc gia) / `Admin1` (tỉnh) / `Admin2` (huyện) |
| `T_res` | string | Độ phân giải thời gian của dòng: `Week` / `Month` / `Year` — **đổi theo giai đoạn trong cùng 1 nước**, xem mục 3.3 |
| `UUID` | string | Định danh duy nhất của bản ghi, do OpenDengue cấp |

### 4.2 `news_rss` (Google News RSS)

| Cột | Kiểu | Mô tả |
|---|---|---|
| `title` | string | Tiêu đề bài báo |
| `link` | string | URL bài báo — gần nhất với khoá duy nhất của 1 bài (dùng để dedup ở Silver) |
| `pubDate` | string | Thời điểm xuất bản theo RSS báo cáo (dạng chuỗi RFC-822) |
| `source` | string | Tên đơn vị xuất bản (VD `"Reuters"`) — **khác với cột lineage `_source`**, xem mục 3.9 |
| `description` | string | Đoạn tóm tắt/snippet của bài, có thể chứa HTML |

Không có trường quốc gia/địa điểm nào — xem mục 3.7.

### 4.3 `who_gho` (WHO GHO xMart, ARBOV Dengue, đã lọc 11 nước SEA ở tầng request)

| Cột | Kiểu | Mô tả |
|---|---|---|
| `COUNTRY` | string | Tên quốc gia đầy đủ theo WHO |
| `ISO3` | string | Mã ISO Alpha-3 quốc gia |
| `YEAR` | bigint | Năm của kỳ báo cáo |
| `DATE_TYPE` | string | Loại độ phân giải thời gian của kỳ báo cáo: `month` / `isoweek` / `epiweek` |
| `DATE_NUM` | bigint | Số thứ tự kỳ báo cáo trong năm (tháng thứ mấy / tuần thứ mấy — cách đánh số chính xác chưa xác minh kỹ, phụ thuộc `DATE_TYPE`) |
| `START_DATE` | string | Ngày bắt đầu kỳ báo cáo |
| `CASES` | bigint | Tổng số ca dengue trong kỳ — **cột sạch nhất (0% null), nên dùng làm chỉ số chính** |
| `CONFIRMED_CASES` | bigint | Số ca đã xác nhận bằng xét nghiệm (tập con của `CASES`) — 70,2% null |
| `DEATHS` | bigint | Số ca tử vong do dengue trong kỳ — 28,4% null |
| `SEVERE_CASES` | bigint | Số ca dengue nặng (theo phân loại WHO) — 97,2% null |
| `SERO_1` .. `SERO_4` | string | Chỉ số liên quan huyết thanh (serotype) DENV-1 đến DENV-4 — ý nghĩa mã hoá cụ thể chưa xác minh kỹ, ~92,9% null mỗi cột |
| `POPULATION` | string | Dân số làm mẫu số tính tỷ lệ — hiện 100% null trong dữ liệu đã lấy |
| `WHO_REGION` | string | Khu vực WHO phân loại (VD `SEARO`, `WPRO`) |

Đã xác nhận thiếu dữ liệu cho Philippines và Brunei (không phải lỗi filter). Xem thêm mục 3.4/3.5/3.6.

## 5. Muốn có dữ liệu Bronze mới nhất để bắt đầu Silver?

```powershell
$env:PYTHONPATH = (Get-Location).Path
.venv\Scripts\python.exe scripts\run_batch.py     # chạy cả 3 nguồn
.venv\Scripts\python.exe scripts\check_bronze.py  # xem lại kết quả
```

Hoặc đọc trực tiếp trong PySpark:

```python
opendengue = spark.read.format("delta").load("data/bronze/opendengue")
news_rss   = spark.read.format("delta").load("data/bronze/news_rss")
who_gho    = spark.read.format("delta").load("data/bronze/who_gho")
```

Muốn xem EDA đầy đủ (chạy được trên Google Colab, không cần cài gì) —
[`notebooks/eda_colab_bronze.ipynb`](../notebooks/eda_colab_bronze.ipynb), đã chạy thật
(29/9/2026). Notebook còn giữ phần EDA của `sg_nea` như tư liệu lịch sử dù nguồn này đã gỡ
khỏi pipeline chính thức.

## 6. Tài liệu liên quan

| File | Nội dung |
|---|---|
| `README.md` | Cài đặt, kiến trúc, cách chạy, giới hạn từng nguồn |
| `docs/bronze-layer-report.md` | Report kỹ thuật đầy đủ: chọn nguồn, Python/PySpark dùng để làm gì, landing/Bronze lưu ra sao, EDA |
| `configs/sources.yaml` | Toàn bộ endpoint/tham số/lịch chạy/bật-tắt của từng nguồn |
