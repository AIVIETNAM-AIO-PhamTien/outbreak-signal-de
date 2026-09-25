# Data profile — Singapore NEA dengue clusters

Hồ sơ dữ liệu của nguồn `sg_nea`, dựng từ bảng `data/bronze/sg_dengue_clusters`.

- **Ngày phân tích**: 2026-09-25
- **Dữ liệu phân tích**: 20 dòng / 2 lô (`20260925T0600Z`, `20260925T1400Z`), cách nhau 8.7 giờ
- **Script tái lập**: `python scripts/eda_bronze_sg_nea.py`

> ⚠️ Mẫu còn rất nhỏ — chỉ 2 lô trong 1 ngày. Các nhận định về *nhịp lấy dữ liệu của chúng ta* chưa đủ cơ sở. Nhận định về *nhịp publish của NEA* thì đáng tin hơn, vì suy ra từ trường `FMEL_UPD_D` vốn trải dài 09-15 → 09-22.

---

## 1. Chất lượng cột

| Cột | Kiểu | Null | Distinct |
|---|---|---|---|
| `batch_id` | string | 0 | 2 |
| `fetched_at` | timestamp | 0 | 2 |
| `object_id` | string | 0 | 10 |
| `locality` | string | 0 | 10 |
| `case_count` | int | 0 | 5 |
| `cluster_updated_at_raw` | string | 0 | 4 |
| `inc_crc` | string | 0 | 10 |
| `polygon_geojson` | string | 0 | 10 |
| `raw_payload` | string | 0 | 10 |
| `source` | string | 0 | 1 |

**Không có null ở bất kỳ cột nào** — ánh xạ schema từ GeoJSON sang bảng hoạt động đúng, không có trường nào bị đọc sai tên.

Lưu ý `object_id` chỉ có 10 giá trị distinct trên 20 dòng: trong cùng một chu kỳ publish của NEA, OBJECTID **giữ nguyên** giữa các lần ta poll. Nó chỉ bị đánh số lại khi NEA publish lại dataset (đã quan sát: `527703` → `528001` qua 2 ngày cho cùng một cụm không hề thay đổi nội dung). Vì vậy nó vẫn không dùng được làm khoá định danh xuyên thời gian.

## 2. Nhịp publish của NEA — phát hiện quan trọng nhất

| Mốc `FMEL_UPD_D` | Số cụm | Cách mốc trước |
|---|---|---|
| 2026-09-15 14:59 | 1 | — |
| 2026-09-17 15:19 | 1 | +2.0 ngày |
| 2026-09-18 15:11 | 4 | +1.0 ngày |
| 2026-09-22 15:25 | 4 | +4.0 ngày |

Hai điều rút ra:

**a. NEA publish quanh 15:00, rất đều.** Bốn mốc đều rơi vào 14:59–15:25. Đây gần như chắc chắn là một tác vụ chạy theo lịch hằng ngày phía NEA, không phải cập nhật ngẫu nhiên.

**b. Nội dung chỉ đổi mỗi 1–4 ngày.** Không phải ngày nào cũng có cụm thay đổi.

**Suy luận về múi giờ** (chưa xác minh được, nhưng có cơ sở): `FMEL_UPD_D` không mang thông tin múi giờ. Nếu là UTC thì 15:00 UTC = 23:00 giờ Singapore — giờ publish lạ với một cơ quan nhà nước. Nếu là SGT (UTC+8) thì là đầu giờ chiều — hợp lý hơn nhiều. Do đó nhiều khả năng đây là **giờ Singapore**. Bronze vẫn lưu nguyên chuỗi gốc ở `cluster_updated_at_raw` để tầng Silver quyết định sau khi có bằng chứng chắc chắn.

**Hệ quả cho nhịp poll**: poll mỗi 60 phút nghĩa là ~24 lần/ngày cho một nguồn đổi vài ngày một lần — khoảng 99% số dòng sẽ là lặp lại. Nên cân nhắc giãn ra, hoặc canh theo khung ~15:00–16:00 giờ Singapore.

## 3. Phân bố số ca

Lô mới nhất (`20260925T1400Z`), 10 cụm đang hoạt động:

| Cụm | Số ca | Cập nhật lần cuối |
|---|---|---|
| Ho Ching Rd / Kang Ching Rd / Tah Ching Rd / Yuan Ching Rd | **75** | 2026-09-22 |
| Bishan St 12 (Blk 110, 115, 116, 118) | 6 | 2026-09-22 |
| Kim Tian Rd (Blk 119B, 119D, 127D, 131B) | 4 | 2026-09-18 |
| Upp Changi Rd East (Changi Ct) | 3 | 2026-09-15 |
| Bt Batok St 21 (Blk 207, 209, 210) | 3 | 2026-09-22 |
| Mandai Est | 2 | 2026-09-18 |
| Canberra Rd (Blk 308) / Sembawang Vista | 2 | 2026-09-18 |
| Farleigh Ave / S'goon Gdn Way | 2 | 2026-09-18 |
| Old Choa Chu Kang Rd | 2 | 2026-09-22 |
| Woodlands Dr 16 (Blk 573A, 573B) | 2 | 2026-09-17 |

```
clusters=10  total_cases=101  min=2  max=75  mean=10.1
```

**Phân bố lệch cực mạnh**: một cụm chiếm 75/101 ca (74%), chín cụm còn lại chỉ 2–6 ca. `mean=10.1` là con số gây hiểu nhầm — không cụm nào có 10 ca cả.

**Hệ quả cho trang cảnh báo sau này**: mọi chỉ số rủi ro **không được dùng trung bình**. Với phân bố kiểu này, trung vị (= 2.5) hoặc chính giá trị lớn nhất mới phản ánh đúng tình hình. Đây cũng là đặc trưng dịch tễ hợp lý — ổ dịch lớn thì hiếm, ổ nhỏ thì nhiều.

Ngưỡng dưới quan sát được là **2 ca** — dường như NEA không công bố cụm dưới 2 ca (cần thêm dữ liệu để khẳng định).

## 4. Thay đổi giữa hai lô

```
20260925T0600Z -> 20260925T1400Z:  unchanged=10  changed=0  new=0  gone=0
```

Sau 8.7 tiếng: **không có gì thay đổi**. Khớp với kết luận ở mục 2 — cụm mới nhất cập nhật ngày 09-22, tức dữ liệu đã "đứng yên" 3 ngày tính tới lúc phân tích.

So sánh này dùng `inc_crc` ghép theo `locality`, **không dùng `object_id`** vì lý do ở mục 1.

---

## Việc cần làm tiếp

| Việc | Cơ sở |
|---|---|
| Xem lại nhịp poll — 60 phút là quá dày | Mục 2: nguồn đổi mỗi 1–4 ngày |
| Xác minh múi giờ của `FMEL_UPD_D` | Mục 2: mới là suy luận, chưa có bằng chứng |
| Thu thập thêm ≥2 tuần dữ liệu rồi chạy lại EDA | Mẫu hiện tại quá nhỏ để kết luận về nhịp |
| Kiểm chứng giả thuyết "ngưỡng tối thiểu 2 ca" | Mục 3: mới thấy min=2, chưa đủ chắc |
