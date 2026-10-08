# Handout: Bàn giao Bronze → Silver — OutbreakSignal DE

Tài liệu cho người bắt đầu tầng **Silver**. Mục đích:
- hiểu tầng Bronze đã làm gì mà không cần đọc lại toàn bộ code;
- có sẵn bảng mô tả dữ liệu (tên cột, kiểu, ý nghĩa) của mọi bảng Bronze.

Tài liệu liên quan:
- [`README.md`](../README.md): cài đặt, kiến trúc, cách chạy.
- [`docs/bronze-layer-report.md`](bronze-layer-report.md): report kỹ thuật.
- [`docs/bronze-fixes.md`](bronze-fixes.md): các lỗi Bronze đã sửa (29–30/09/2026) và vấn đề của
  chính nguồn dữ liệu.

Một bản Silver/Gold thử độ khả thi đã được dựng trên nhánh `feat/silver-gold` (tài liệu
`docs/silver-handover.md` trên nhánh đó). Người làm Silver nên đọc qua để tham khảo cách xử lý
các vấn đề ở mục 3.

---

## 1. Bronze đã làm gì (tóm tắt 1 phút)

- **Mục tiêu dự án:** phát hiện sớm khu vực có nguy cơ bùng dịch sốt xuất huyết ở 11 nước Đông
  Nam Á.
- **Các nguồn:** 8 nguồn, 10 bảng Delta. Mỗi nguồn ghi bảng riêng; Bronze **không join, không
  merge**.
- **Kỹ thuật:** batch ingestion, không phải streaming.
  1. Python thuần tải dữ liệu về `data/landing/` (bản gốc, y nguyên byte-for-byte).
  2. PySpark đọc lại, thêm cột lineage, ghi Delta vào `data/bronze/<bảng>/`.
- **Nguyên tắc Bronze:** giữ nguyên trạng dữ liệu nguồn.
  - Không đổi tên cột, không chuẩn hoá giá trị, không xoá trùng.
  - **Mọi cột đều là `string`**, kể cả cột số. Ép kiểu là việc của Silver.
  - Lý do: để Spark tự suy kiểu thì cột đang toàn null sẽ thành string, hôm nào có số sẽ đổi
    kiểu và làm vỡ bước ghi Delta.
- **Ngoại lệ lọc phạm vi:** `opendengue` và `hdx_cod_ps` lọc còn 11 nước trước khi ghi Bronze.
  Đây là chọn **phạm vi thu thập**, không phải biến đổi giá trị; landing vẫn giữ 100% file gốc.
- **Idempotency**, chia theo kiểu nguồn:
  - **Theo ngày** (`news_rss`, `who_gho`, `sg_nea`): dựng lại partition `ingestion_date` từ mọi
    file landing của ngày đó, ghi đè bằng `replaceWhere`.
  - **Theo phiên bản** (`opendengue`, `hdx_*`, `trends_th_*`, `ph_doh`): partition theo phiên
    bản. Nếu phiên bản và dấu vân tay file đã có trong Bronze thì **bỏ qua**, không tải lại.

## 2. Vai trò từng nguồn cho Silver

| Bảng | Vai trò | Ghi chú |
|---|---|---|
| `opendengue` | Số ca lịch sử, cấp quốc gia và tỉnh | Phủ 11/11 nước, từ 1960, nhưng trễ ~17 tháng. Số liệu cấp tỉnh của VN/PHL/KHM/LAO chỉ tới 2010 |
| `who_gho` | Số ca **gần đây**, cấp quốc gia | Trễ ~5 tuần. Không có Philippines, Brunei |
| `news_rss` | Tín hiệu sớm | Nguồn **duy nhất** gần real-time. 11 feed theo nước, ngôn ngữ bản xứ |
| `hdx_cod_ab` (+ `_geometry`) | Danh mục đơn vị hành chính (P-code) | Khoá đích để quy các nguồn cấp tỉnh về một bộ mã. **Với VN phải đi qua `vn_province_crosswalk`** — P-code của `hdx_cod_ab` (34 tỉnh mới, `VN01`) và `hdx_cod_ps` (63 tỉnh cũ, `VN101`) giao nhau bằng 0 |
| `vn_province_crosswalk` | Bảng nối 64 tỉnh cũ VN → 34 tỉnh mới | Bắt buộc trước mọi phép join cấp tỉnh của Việt Nam. Phủ cả NQ 202/2025/QH15 và Hà Tây (2008) |
| `geoboundaries_adm` | Ranh giới vá lấp cho nước COD-AB không có | Hiện chỉ Brunei (4 district ADM1). Dùng `shapeID` riêng, **không phải** P-code OCHA |
| `hdx_cod_ps` | Dân số theo đơn vị | Mẫu số cho ca / 100.000 dân |
| `trends_th_weekly`, `trends_th_province` | Số ca Thái Lan, tuần × 77 tỉnh, 2016–2025 | Nguồn cấp tỉnh **gần đây duy nhất** |
| `ph_doh` | Số ca Philippines, tuần × tỉnh | Tháng đầy đủ cuối cùng 12/2020 |
| `sg_nea` | Cụm dịch Singapore đang hoạt động | Có polygon, cấp cụm phố |

## 3. Vấn đề đã biết mà Silver cần xử lý

Mỗi dòng là một quyết định thiết kế Silver phải đưa ra, không phải bug của Bronze. Các con số đo
trên dữ liệu thật ngày 29/09/2026.

1. **Không có khoá địa lý chung giữa các nguồn.**
   - `opendengue` dùng tên nước UN (`VIET NAM`); `who_gho` dùng `ISO3`.
   - `news_rss` chỉ có `feed_country`, tức nước của feed chứ không phải nước bài báo nói tới.
   - Cấp tỉnh: `opendengue` có `RNE_iso_code` (ISO 3166-2 theo Natural Earth); TRENDS và COD
     dùng P-code.

   → Cần chuẩn hoá khoá: cấp nước về ISO3, cấp tỉnh về P-code của COD.
2. **Thời gian không đồng nhất, kể cả trong cùng một nguồn.**
   - `opendengue.T_res` là Week, Month hoặc Year, và đổi theo giai đoạn trong cùng một nước.
   - `who_gho.DATE_TYPE` là `month`, `isoweek` hoặc `epiweek`.
   - Có 60 trường hợp cùng địa điểm và cùng tháng có nhiều chuỗi song song.

   → Mỗi (nguồn, địa điểm, tháng) phải chọn đúng một chuỗi, nếu không sẽ **cộng trùng**.
3. **`opendengue.UUID` không phải khoá dòng.** Chỉ có 234 giá trị cho 70.557 dòng; đây là mã
   tài liệu nguồn.
4. **`opendengue`: Philippines có `adm_1` là vùng, `adm_2` là tỉnh.** `RNE_iso_code` của
   Philippines sai trên diện rộng (51 địa danh có mã không khớp tên), nên cần nối cả theo tên.
5. **`who_gho`: khoá dòng là `(ISO3, YEAR, DATE_TYPE, DATE_NUM)`.** Bộ `(ISO3, START_DATE,
   DATE_TYPE)` không duy nhất.
   - `START_DATE` trống ở 36 dòng (IDN 2007–2009); suy lại được từ `YEAR` + `DATE_NUM`.
   - Indonesia xuất hiện ở 2 WHO region.
6. **`who_gho` có lỗi từ phía nguồn**, Bronze giữ nguyên:
   - 5 dòng có kỳ ở **tương lai** (MYS, SGP, năm 2027–2029);
   - 1 dòng `CONFIRMED_CASES` > `CASES` (KHM 2025);
   - tháng gần nhất có thể chưa báo cáo đủ (IDN: T6 9.264 → T7 3.514 → T8 67 ca).
7. **`who_gho`: nhiều cột gần như trống.**
   - `POPULATION` 100% null, `SEVERE_CASES` 97,2%, `SERO_1..4` ~92,9%, `CONFIRMED_CASES` 70,2%.
   - `CASES` không null ở dòng nào, nên dùng làm chỉ số chính.
8. **`news_rss`: mỗi lần fetch là một quan sát riêng.** Bài lặp giữa các lần fetch là cố ý.
   - Dedup theo `guid`; dòng cũ chưa có `guid` thì dùng `link`.
   - Thời điểm thấy bài lấy từ `_fetched_at`, **không** lấy từ `_ingested_at` (bị ghi lại khi
     dựng lại partition).
9. **`news_rss`: nước của bài.** Bài trên trang đa ngôn ngữ (vd Vietnam.vn) có thể nằm trong
   feed của nước khác. Nên lấy hợp của `feed_country` và nước được nhắc trong bài; bản thử đạt
   89% số bài gắn được nước.
10. **`news_rss`: độ phủ không đều giữa các feed.** Ở lần fetch 29/09/2026 (UTC):
    - Brunei và Lào trả **0 bài**, Timor-Leste 1 bài, Campuchia 3 bài;
    - Indonesia, Philippines, Singapore chạm **trần 100 bài** của Google, nên có thể bị cắt
      bớt. Trường hợp này được ghi vào `warnings` của metadata.

    Số bài theo nước không so trực tiếp được với nhau.
11. **`news_rss.source` là tên nhà xuất bản**, dễ nhầm với cột lineage `_source` (luôn là
    `"news_rss"`).
12. **Việt Nam sáp nhập 63 → 34 tỉnh từ 01/07/2025.** → **đã có bảng nối.**
    - `hdx_cod_ab` chỉ còn 34 tỉnh mới, trong khi `hdx_cod_ps` và `opendengue` dùng 63 tỉnh cũ.
    - Hai hệ P-code **giao nhau bằng 0** (`VN66` vs `VN605`), nên không join trực tiếp được.
      Nguy hiểm hơn số lượng: tên trùng nhau vẫn là hai thứ khác nhau — "Dak Lak" cũ 1,93 triệu
      dân, "Dak Lak" mới = Đắk Lắk + Phú Yên = 2,80 triệu dân. Join theo tên **chạy được mà ra
      số sai ~45%, không báo lỗi**.
    - Dùng bảng `vn_province_crosswalk` (64 dòng → 34 đơn vị): chuẩn hoá tên → `old_name`,
      tra `new_pcode`, rồi `GROUP BY new_pcode` cộng dồn. Luôn quy **cũ → mới**; chiều ngược
      lại là phép tách, không khôi phục được.
    - **Chưa cần SCD2**: toàn bộ dữ liệu VN trong OpenDengue kết thúc 31/03/2025, tức 100%
      thuộc kỳ cũ. SCD2 chỉ cần khi có dữ liệu sau 01/07/2025 đổ về.
13. **`hdx_cod_ab`:** Indonesia là bản 2020 (34 tỉnh, chưa có 4 tỉnh Papua mới).
    P-code của Philippines lệch giữa `hdx_cod_ab` và `hdx_cod_ps`.
    - **Brunei**: HDX không có COD-AB (`cod-ab-brn`/`-bru`/`-brunei-darussalam` đều HTTP 404,
      kiểm chứng 08/10/2026). Đã vá bằng `geoboundaries_adm` — 4 district ADM1, khớp 4/4 với
      `adm_1_name` của OpenDengue. Để **bảng riêng** vì `shapeID` không phải P-code OCHA.
    - **Singapore**: vẫn không có và **không vá** — geoBoundaries cho 5 *region* trong khi
      OpenDengue chỉ có 1 dòng `CENTRAL SINGAPORE`, khác cấp. Singapore đã có `sg_nea`.
14. **`hdx_cod_ps` có hai dạng bảng trong cùng một bảng Delta**, phân biệt bằng `_resource`:
    - dạng dài (`Gender`, `Age_range`, `Population`);
    - dạng rộng của PHL cấp 2 (`T_TL`, `F_00_04`…).
15. **`ph_doh`:** thành phố lớn (Makati, Baguio…) được ghi tách riêng khỏi tỉnh, khoảng 27% số
    dòng.

## 4. Data dictionary

### 4.0 Cột lineage (Bronze thêm vào, không có trong dữ liệu gốc)

| Cột | Kiểu | Có ở | Mô tả |
|---|---|---|---|
| `_source` | string | mọi bảng | Tên nguồn ingest |
| `_ingested_at` | string | mọi bảng | Thời điểm PySpark ghi dòng vào Bronze (ISO-8601 UTC). **Bị ghi lại** mỗi lần dựng lại partition |
| `_fetched_at` | string | `news_rss`, `who_gho`, `sg_nea`, `opendengue` | Thời điểm **gọi API / tải file** (ISO-8601 UTC), ghi ngay trong file landing. Không bị ghi đè |
| `_source_file` | string | mọi bảng | File landing mà dòng này đọc ra, tính từ thư mục `landing/` (vd `news_rss/2026-09-29/…jsonl`) |
| `ingestion_date` | string | mọi bảng | Ngày nạp `YYYY-MM-DD`. Là cột phân vùng của các nguồn theo ngày |
| `release`, `_file_sha` | string | `opendengue` | Tên release (vd `V1.3`, là cột phân vùng) và git blob sha của file zip |
| `_version` | string | `hdx_*`, `ph_doh` | `last_modified` của resource trên HDX (cột phân vùng của `ph_doh`) |
| `_release`, `_file_md5` | string | `trends_th_*` | Record id Zenodo (cột phân vùng) và md5 của file |

### 4.1 `opendengue` (Spatial_extract, đã lọc 11 nước), 23 cột

| Cột | Kiểu | Mô tả |
|---|---|---|
| `adm_0_name` | string | Tên quốc gia (UN name, viết hoa, vd `"VIET NAM"` có dấu cách) |
| `adm_1_name` | string | Đơn vị cấp 1 (tỉnh; với Philippines là **vùng**) |
| `adm_2_name` | string | Đơn vị cấp 2 (huyện; với Philippines là **tỉnh**) |
| `full_name` | string | Tên đầy đủ ghép các cấp hành chính |
| `ISO_A0` | string | Mã ISO Alpha-3 của quốc gia |
| `FAO_GAUL_code` | string | Mã đơn vị theo FAO GAUL |
| `RNE_iso_code` | string | Mã ISO 3166-2 theo `rnaturalearth` (bài báo gốc OpenDengue, Sci Data 2024). Với Philippines sai nhiều, xem mục 3.4 |
| `IBGE_code` | string | Mã hành chính của Brazil; với Đông Nam Á không có ý nghĩa |
| `calendar_start_date`, `calendar_end_date` | string | Ngày bắt đầu và kết thúc kỳ báo cáo |
| `Year` | string | Năm của kỳ báo cáo |
| `dengue_total` | string | Tổng số ca trong kỳ và khu vực |
| `case_definition_standardised` | string | Định nghĩa ca (`Total`, `Suspected and confirmed`, `Confirmed`) |
| `S_res` | string | Độ phân giải không gian: `Admin0` / `Admin1` / `Admin2` |
| `T_res` | string | Độ phân giải thời gian: `Week` / `Month` / `Year`, đổi theo giai đoạn |
| `UUID` | string | Mã **tài liệu nguồn** của bản ghi, không phải khoá dòng (mục 3.3) |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_fetched_at`, `release`, `_file_sha` |

### 4.2 `news_rss` (Google News RSS), 17 cột

| Cột | Kiểu | Mô tả |
|---|---|---|
| `guid` | string | Định danh bài của RSS. **Khoá tự nhiên**, dùng để dedup |
| `title` | string | Tiêu đề (thường có dạng "… - Tên báo") |
| `link` | string | URL bài qua Google News |
| `pubDate` | string | Thời điểm đăng, chuỗi RFC-822 (vd `Mon, 29 Sep 2026 07:00:00 GMT`) |
| `source` | string | Tên nhà xuất bản. **Khác** cột lineage `_source` |
| `source_url` | string | URL trang chủ nhà xuất bản (thuộc tính `url` của tag `<source>`) |
| `description` | string | Đoạn tóm tắt, có chứa HTML |
| `raw_payload` | string | XML gốc của cả `<item>`, dùng để lấy lại trường chưa được tách ra |
| `feed_country` | string | ISO3 của feed đã trả về bài này |
| `feed_hl`, `feed_gl` | string | Ngôn ngữ và vùng của feed (vd `vi`, `VN`) |
| `feed_query` | string | Truy vấn đã gửi (vd `sốt xuất huyết when:7d`) |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_fetched_at` |

64 dòng trong partition `2026-09-29` không có `guid`, `_fetched_at` và `feed_*`. Chúng đến từ file
`google_news_20260929T142729Z.jsonl`, tải trước khi Bronze được sửa để giữ các trường này. Với
những dòng đó, dedup theo `link`, còn thời điểm lấy từ run_id trong tên file.

### 4.3 `who_gho` (WHO GHO xMart, ARBOV Dengue, lọc 11 nước ở tầng request), 21 cột

| Cột | Kiểu | Mô tả |
|---|---|---|
| `COUNTRY` | string | Tên quốc gia theo WHO |
| `ISO3` | string | Mã ISO Alpha-3 |
| `WHO_REGION` | string | Khu vực WHO (`SEARO`, `WPRO`) |
| `YEAR` | string | Năm của kỳ báo cáo |
| `DATE_TYPE` | string | `month` / `isoweek` / `epiweek` |
| `DATE_NUM` | string | Số thứ tự tháng hoặc tuần trong năm, theo `DATE_TYPE`. Đã đối chiếu: khớp `START_DATE` ở mọi dòng có `START_DATE` |
| `START_DATE` | string | Ngày bắt đầu kỳ. Trống ở 36 dòng |
| `CASES` | string | Tổng số ca. Không null ở dòng nào; chỉ số chính |
| `CONFIRMED_CASES` | string | Số ca xác nhận xét nghiệm, 70,2% null |
| `DEATHS` | string | Số ca tử vong, 28,4% null |
| `SEVERE_CASES` | string | Số ca nặng, 97,2% null |
| `SERO_1` .. `SERO_4` | string | Thông tin serotype DENV-1..4, ~92,9% null mỗi cột |
| `POPULATION` | string | Dân số, 100% null |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_fetched_at` |

### 4.4 `hdx_cod_ab` (đơn vị hành chính COD, 9 nước), 57 cột, phân vùng `iso3`

Mỗi dòng là một đơn vị hành chính, lấy từ một sheet của file XLSX. Cột `_sheet` cho biết sheet
nguồn (`admin1`, `admin2`, `adminpoints`…); các cột khác nhau theo cấp.

| Cột | Kiểu | Mô tả |
|---|---|---|
| `iso3`, `_sheet`, `admin_level` | string | Nước, sheet nguồn, cấp hành chính (của dòng `adminpoints`) |
| `adm0_pcode` … `adm5_pcode` | string | P-code từng cấp: khoá đơn vị |
| `adm{n}_name` | string | Tên tiếng Anh/Latin |
| `adm{n}_name1..3`, `lang1..3` | string | Tên bằng ngôn ngữ bản xứ và mã ngôn ngữ tương ứng |
| `center_lat`, `center_lon` | string | Toạ độ tâm (sheet cấp hành chính) |
| `x_coord`, `y_coord`, `name*` | string | Toạ độ và tên của điểm đại diện (sheet `adminpoints`) |
| `valid_on`, `valid_to`, `version`, `area_sqkm` | string | Ngày hiệu lực, phiên bản COD, diện tích |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_version` |

`hdx_cod_ab_geometry` (11 cột) chỉ có Việt Nam: 34 tỉnh mới, gồm `adm1_pcode`, `adm1_name`,
`geometry` (GeoJSON dạng chuỗi), `raw_payload` và `valid_on`.

### 4.5 `hdx_cod_ps` (dân số COD), 84 cột, phân vùng `_resource`

| Cột | Kiểu | Mô tả |
|---|---|---|
| `_resource`, `_source_resource`, `_admin_level` | string | Resource HDX nguồn (bảng toàn cầu cấp 1, hoặc PHL cấp 2) và cấp hành chính |
| `ISO3`, `ADM1_PCODE`, `ADM1_NAME` … `ADM4_*` | string | Mã và tên đơn vị (dạng dài) |
| `Population_group`, `Gender`, `Age_range`, `Age_min`, `Age_max` | string | Nhóm dân số. Tổng dân số lấy ở dòng `Gender = "all"` và `Age_range = "all"` |
| `Population`, `Reference_year`, `Source`, `Contributor` | string | Giá trị dân số, năm tham chiếu, nguồn |
| `ADM0_*`, `ADM2_*`, `year`, `T_TL`, `F_TL`, `M_TL`, `{F,M,T}_{tuổi}` | string | Dạng rộng của PHL cấp 2: tổng (`T_TL`), theo giới và nhóm tuổi |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_version` |

### 4.6 `trends_th_weekly` (24 cột) và `trends_th_province` (22 cột)

| Cột | Kiểu | Mô tả |
|---|---|---|
| `p_code`, `province`, `health_region`, `country` | string | P-code (khớp COD), tên tỉnh, vùng y tế |
| `year`, `month`, `epi_week`, `week_start`, `week_end` | string | Kỳ báo cáo theo tuần dịch tễ |
| `dengue_fever_df`, `dengue_haemorrhagic_fever_dhf`, `dengue_shock_syndrome_dss`, `dengue_total` | string | Số ca theo thể bệnh và tổng |
| `dengue_rate_per_100_000` | string | Tỉ lệ trên 100.000 dân do nguồn tính |
| `chikungunya*`, `hfmd*` | string | Bệnh khác cùng file, ngoài phạm vi dengue |
| `centroid_latitude`, `centroid_longitude`, `area_km`, `population_2016..2025` | string | Chỉ ở `trends_th_province`: toạ độ, diện tích, dân số theo năm |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_release`, `_file_md5` |

### 4.7 `ph_doh` (Philippines DOH), 10 cột

| Cột | Kiểu | Mô tả |
|---|---|---|
| `loc` | string | Tên tỉnh **hoặc thành phố lớn**, viết hoa (mục 3.15) |
| `Region` | string | Vùng, vd `REGION V-BICOL REGION`, `NATIONAL CAPITAL REGION`. Viết hoa không đồng nhất (`Region I-…`), và Eastern Visayas bị nguồn ghi là `REGION VII-EASTERN VISAYAS` (đúng là VIII), trùng số với Central Visayas. Đừng dùng số La Mã làm khoá |
| `date` | string | Ngày của tuần báo cáo, dạng `M/D/YYYY` |
| `cases`, `deaths` | string | Số ca và số ca tử vong trong tuần |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_version` |

Dòng HXL (`#…`) ngay dưới header của file gốc đã bị bỏ khi nạp.

### 4.8 `sg_nea` (cụm dịch Singapore), 19 cột

Mỗi dòng là một cụm dịch đang hoạt động tại thời điểm fetch.

Partition của một ngày phản ánh **lần fetch mới nhất trong ngày**. Nếu lần đó trả 0 cụm dịch,
partition ngày đó **rỗng**: các dòng cũ bị xoá, còn file GeoJSON gốc vẫn nằm trong landing.
Vì vậy Silver không phân biệt được "0 cụm dịch" với "hôm đó không chạy" chỉ bằng bảng này. Cần
đọc thêm metadata `data/metadata/sg_nea/`: `status = success` với `record_count = 0` nghĩa là
0 cụm dịch.

| Cột | Kiểu | Mô tả |
|---|---|---|
| `LOCALITY`, `NAME` | string | Mô tả khu vực cụm dịch |
| `CASE_SIZE` | string | Số ca trong cụm |
| `HOMES`, `PUBLIC_PLACES`, `CONSTRUCTION_SITES` | string | Các trường thuộc tính của NEA (giữ nguyên tên gốc) |
| `INC_CRC`, `OBJECTID`, `FMEL_UPD_D`, `HYPERLINK`, `SHAPE.AREA`, `SHAPE.LEN` | string | Mã, ngày cập nhật, liên kết và thông số hình học do NEA cung cấp |
| `geometry` | string | Polygon GeoJSON dạng chuỗi |
| `raw_payload` | string | Feature GeoJSON gốc |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_fetched_at` |

### 4.9 `vn_province_crosswalk` (bảng nối 64 tỉnh cũ VN → 34 tỉnh mới), phân vùng `_version`

64 dòng, mỗi dòng là **một đơn vị cấp tỉnh cũ** và đích của nó sau sáp nhập. Không phải nguồn
tải từ mạng: seed file `configs/reference/vn_province_merge_2025.csv` trong repo, vì đây là dữ
liệu pháp lý cố định và HDX chỉ phát hành bản hiện hành (34 dòng, `valid_to` đều rỗng).

| Cột | Kiểu | Mô tả |
|---|---|---|
| `old_name` | string | Tên tỉnh cũ, chính tả chuẩn `hdx_cod_ps` (vd `Thua Thien Hue`) |
| `old_pcode_ps` | string | P-code cũ 5 ký tự để join `hdx_cod_ps` (vd `VN411`). Rỗng với Hà Tây |
| `new_name`, `new_pcode` | string | Tên và P-code mới 4 ký tự, khớp đúng `hdx_cod_ab` (vd `Hue`, `VN46`) |
| `effective_from` | string | `2025-07-01` (NQ 202) hoặc `2008-08-01` (Hà Tây) |
| `legal_basis` | string | `NQ 202/2025/QH15` hoặc `NQ 15/2008/QH12` |
| `source_aliases` | string | Chính tả của nguồn khác, ngăn bằng `\|` — hiện là `adm_1_name` của OpenDengue (vd `THUA THIEN - HUE`) |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_version`, `iso3` |

Mỗi lần chạy, job **đối chiếu ngược** `new_pcode` với bảng `hdx_cod_ab` thật: thừa hoặc thiếu
một đơn vị là FAILED ngay, không ghi bảng nối lệch. `_version` = SHA-256 của seed (12 ký tự đầu).

Cách dùng ở Silver: chuẩn hoá tên nguồn → `old_name` (qua `source_aliases`), tra `new_pcode`,
rồi `GROUP BY new_pcode` cộng dồn số ca và dân số. **Chỉ đi cũ → mới.**

### 4.10 `geoboundaries_adm` (ranh giới vá lấp), phân vùng `_partition`

Chỉ các nước `hdx_cod_ab` không phủ. Hiện là Brunei, 4 district cấp ADM1.

| Cột | Kiểu | Mô tả |
|---|---|---|
| `iso3`, `admin_level`, `_partition` | string | Nước, cấp hành chính, phân vùng dạng `BRN_ADM1` |
| `shape_id`, `shape_name`, `shape_iso` | string | Mã và tên đơn vị. `shape_id` là mã riêng của geoBoundaries, **không phải P-code OCHA** |
| `shape_group`, `shape_type` | string | Mã nước và cấp, theo cách geoBoundaries gọi |
| `boundary_canonical`, `boundary_year`, `boundary_license` | string | Tên gọi chính thức của cấp (`Districts`), năm đại diện, license |
| `geometry` | string | Polygon GeoJSON dạng chuỗi |
| `raw_payload` | string | Properties gốc của feature |
| `_provider` | string | Luôn là `geoboundaries` — cột để Silver phân biệt với dòng COD chính thức |
| + lineage | | `_source`, `_ingested_at`, `_source_file`, `ingestion_date`, `_version` |

`_version` = `boundaryID` của geoBoundaries (vd `BRN-ADM1-89281809`).

## 5. Lấy dữ liệu Bronze mới nhất

```powershell
.venv\Scripts\python.exe scripts\run_batch.py      # mọi nguồn đang bật
.venv\Scripts\python.exe scripts\check_bronze.py   # xem lại kết quả
```

Hoặc đọc trực tiếp trong PySpark:

```python
who_gho = spark.read.format("delta").load("data/bronze/who_gho")
latest = who_gho.where("ingestion_date = (SELECT max(ingestion_date) FROM delta.`data/bronze/who_gho`)")
```

- `who_gho`, `sg_nea`: mỗi partition ngày là **một snapshot đầy đủ**, nên đọc partition mới nhất.
- `news_rss`: một partition ngày chứa **mọi lần fetch** trong ngày đó (mỗi lần thêm khoảng 450
  dòng). Đọc **mọi partition** rồi dedup. `_fetched_at` phân biệt các lần fetch, còn
  `_ingested_at` thì giống nhau ở mọi dòng của partition.
- Ngày partition là ngày **UTC**: chạy lúc 00:46 giờ Việt Nam ngày 30/09 vẫn ghi vào partition
  `2026-09-29`.
- Với `opendengue`, đọc release có version cao nhất.

Muốn xem EDA (chạy được trên Google Colab) thì mở
[`notebooks/eda_colab_bronze.ipynb`](../notebooks/eda_colab_bronze.ipynb). Notebook này làm trên
Bronze **trước** đợt sửa ngày 29–30/09/2026, nên chưa có các cột mới.

## 6. Tài liệu liên quan

| File | Nội dung |
|---|---|
| `README.md` | Cài đặt, kiến trúc, cách chạy, giới hạn từng nguồn |
| `docs/bronze-layer-report.md` | Report kỹ thuật: chọn nguồn, landing/Bronze, EDA |
| `docs/bronze-fixes.md` | Lỗi Bronze đã sửa, nguồn mới, vấn đề của chính nguồn dữ liệu |
| `configs/sources.yaml` | Endpoint, tham số, lịch chạy, bật/tắt từng nguồn |
