"""Test Silver news_articles: dedup qua nhieu lan fetch, thoi diem thay bai, parse ngay."""

import pytest
from pyspark.sql import functions as F

from transform.common import small_frame
from transform.silver import news as sn

pytestmark = pytest.mark.integration

SCHEMA = (
    "title string, link string, pubDate string, source string, description string, "
    "_source_file string, _ingested_at string, ingestion_date string"
)
LANDING = "file:///x/data/landing/news_rss/2026-09-29/"
# _ingested_at giong nhau o moi dong - dung nhu Bronze that (bi ghi de khi dung lai partition).
STAMP = "2026-09-29T13:00:00+00:00"


@pytest.fixture
def bronze(spark):
    rows = [
        # Cung mot bai, thay o 2 lan fetch
        ("Dengue rises in Vietnam", "https://n/a1", "Mon, 28 Sep 2026 07:00:00 GMT", "VnExpress",
         '<a href="x">Dengue rises</a>',
         LANDING + "google_news_20260929T060000Z.jsonl", STAMP, "2026-09-29"),
        # Lan fetch sau (moi nhat) - Silver lay noi dung theo lan nay
        ("Dengue rises in Vietnam", "https://n/a1", "Mon, 28 Sep 2026 07:00:00 GMT", "VnExpress",
         '<a href="x">Dengue rises</a>&nbsp;<font>VnExpress</font>',
         LANDING + "google_news_20260929T120000Z.jsonl", STAMP, "2026-09-29"),
        # Bai cu tu 2020, khong gan duoc nuoc
        ("Global dengue report", "https://n/a2", "Sat, 17 Oct 2020 07:00:00 GMT", "WHO",
         "<p>report</p>", LANDING + "google_news_20260929T120000Z.jsonl", STAMP, "2026-09-29"),
        # pubDate hong + ten file khong co run_id
        ("Thai outbreak", "https://n/a3", "khong phai ngay", "Bangkok Post", "",
         LANDING + "manual_upload.jsonl", STAMP, "2026-09-29"),
    ]
    return small_frame(spark, rows, SCHEMA)


def by_link(frame, link: str):
    """Lay dong theo link, cac cot timestamp doi thanh chuoi UTC.

    collect() doi timestamp sang gio dia phuong cua may chay test (VN = UTC+7),
    nen format bang Spark (session UTC) truoc de so sanh on dinh tren moi may.
    """
    for column in ("published_at", "first_seen_at", "last_seen_at"):
        frame = frame.withColumn(column, F.date_format(column, "yyyy-MM-dd HH:mm:ss"))
    return frame.where(frame.link == link).first()


class TestDedupVaThoiDiem:
    def test_moi_bai_mot_dong(self, bronze) -> None:
        assert sn.build_news_articles(bronze).count() == 3

    def test_thoi_diem_thay_bai_lay_tu_ten_file(self, bronze) -> None:
        row = by_link(sn.build_news_articles(bronze), "https://n/a1")
        assert row["first_seen_at"] == "2026-09-29 06:00:00"
        assert row["last_seen_at"] == "2026-09-29 12:00:00"
        assert row["seen_count"] == 2

    def test_ten_file_khong_co_run_id_thi_lui_ve_ingested_at(self, bronze) -> None:
        row = by_link(sn.build_news_articles(bronze), "https://n/a3")
        assert sn.FLAG_FETCH_TIME_FALLBACK in row["dq_flags"]
        assert row["first_seen_at"] == "2026-09-29 13:00:00"


class TestNoiDung:
    def test_parse_pubdate_va_bo_html(self, bronze) -> None:
        row = by_link(sn.build_news_articles(bronze), "https://n/a1")
        assert row["published_at"] == "2026-09-28 07:00:00"
        assert row["description_text"] == "Dengue rises VnExpress"

    def test_pubdate_hong_bi_gan_co(self, bronze) -> None:
        row = by_link(sn.build_news_articles(bronze), "https://n/a3")
        assert row["published_at"] is None
        assert sn.FLAG_PUBDATE_UNPARSED in row["dq_flags"]

    def test_gan_nuoc_bang_regex_cua_spark(self, bronze) -> None:
        articles = sn.build_news_articles(bronze)
        assert by_link(articles, "https://n/a1")["countries"] == ["VNM"]
        assert by_link(articles, "https://n/a2")["countries"] == []
        assert by_link(articles, "https://n/a3")["countries"] == ["THA"]

    def test_bai_cu_khong_phai_tin_moi(self, bronze) -> None:
        articles = sn.build_news_articles(bronze)
        assert by_link(articles, "https://n/a1")["is_recent"] is True
        assert by_link(articles, "https://n/a2")["is_recent"] is False


NEW_SCHEMA = (
    "title string, link string, guid string, pubDate string, source string, "
    "source_url string, description string, feed_country string, _fetched_at string, "
    "_source_file string, _ingested_at string, ingestion_date string"
)


class TestBronzeMoi:
    """Bronze sau khi sua 29/9/2026: co guid, feed_country, _fetched_at."""

    @pytest.fixture
    def bronze_moi(self, spark):
        rows = [
            # Cung 1 bai (cung guid) tra ve tu 2 feed, link khac nhau chut
            ("Dengue o Ha Noi", "https://n/x?a=1", "G1", "Mon, 28 Sep 2026 07:00:00 GMT",
             "VnExpress", "https://vnexpress.net", "", "VNM", "2026-09-29T08:00:00+00:00",
             "news_rss/2026-09-29/google_news_20260929T080000Z.jsonl", STAMP, "2026-09-29"),
            ("Dengue o Ha Noi", "https://n/x?a=2", "G1", "Mon, 28 Sep 2026 07:00:00 GMT",
             "VnExpress", "https://vnexpress.net", "", "SGP", "2026-09-29T09:30:00+00:00",
             "news_rss/2026-09-29/google_news_20260929T093000Z.jsonl", STAMP, "2026-09-29"),
        ]
        return small_frame(spark, rows, NEW_SCHEMA)

    def test_dedup_theo_guid(self, bronze_moi) -> None:
        assert sn.build_news_articles(bronze_moi).count() == 1

    def test_nuoc_gom_feed_va_tu_khoa(self, bronze_moi) -> None:
        row = sn.build_news_articles(bronze_moi).first()
        assert row["feed_countries"] == ["SGP", "VNM"]
        assert row["countries"] == ["SGP", "VNM"]
        assert row["publisher_url"] == "https://vnexpress.net"

    def test_thoi_diem_lay_tu_fetched_at(self, bronze_moi) -> None:
        frame = sn.build_news_articles(bronze_moi)
        row = frame.select(F.date_format("first_seen_at", "HH:mm").alias("t"),
                           "dq_flags").first()
        assert row["t"] == "08:00"
        assert row["dq_flags"] == []
