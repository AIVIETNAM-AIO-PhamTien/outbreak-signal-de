"""Ghi du lieu vao tang Bronze.

Quy tac Bronze, ap dung cho ca ba nguon:

  - KHONG doi ten cot, KHONG loc, KHONG xoa trung, KHONG ep kieu du lieu.
  - Chi them cot lineage de truy nguoc nguon goc.
  - Moi viec lam sach / chuan hoa de tang Silver.

Vi du: nguon ghi {"country": "VNM", "confirmed_cases": 120} thi Bronze phai
giu nguyen "VNM" va nguyen ten cot "confirmed_cases". Doi thanh "Vietnam" hay
doi ten thanh "cases" la viec cua Silver.

Dinh dang: Delta, phan vung theo `ingestion_date`. Chon Delta thay vi Parquet
tran vi can `replaceWhere` de chay lai mot ngay ma khong nhan doi du lieu -
xem write_bronze() ben duoi.
"""

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.common.paths import PARTITION_COLUMN, bronze_path
from ingestion.common.validation import IngestionValidationError

# Cac cot lineage them vao moi bang Bronze. Day la metadata muc TUNG DONG
# (dong nay tu dau ra), khac voi metadata muc LAN CHAY o ingestion/common/metadata.py.
LINEAGE_COLUMNS = ("_source", "_ingested_at", "_source_file")


def add_bronze_columns(
    df: DataFrame,
    source: str,
    ingestion_date: str,
    ingested_at: datetime,
    with_input_file: bool = True,
) -> DataFrame:
    """Them cot lineage va cot phan vung, giu nguyen toan bo cot goc.

    Day la thay doi DUY NHAT ma Bronze duoc phep lam voi du lieu nguon.

    Args:
        df: DataFrame doc tu landing, con nguyen schema goc.
        source: Ten nguon, ghi vao cot _source.
        ingestion_date: Ngay phan vung dang YYYY-MM-DD.
        ingested_at: Thoi diem chay, ghi vao cot _ingested_at.
        with_input_file: True thi sinh cot _source_file bang input_file_name().
            Dat False khi DataFrame duoc tao bang createDataFrame() thay vi
            doc truc tiep tu file - luc do Spark khong biet file nguon nen
            input_file_name() tra ve chuoi rong, va nguon phai tu dien cot
            _source_file khi build rows.

    Returns:
        DataFrame da san sang ghi vao Bronze.
    """
    enriched = df.withColumn("_source", F.lit(source)).withColumn(
        "_ingested_at", F.lit(ingested_at.isoformat())
    )
    if with_input_file:
        # Cho biet moi dong den tu file nao - can thiet voi nguon chay nhieu
        # lan trong ngay, vi mot phan vung ngay gom nhieu lan fetch. Chi giu
        # phan tinh tu thu muc landing/ (vd "news_rss/2026-09-29/x.jsonl"):
        # duong dan tuyet doi "file:///C:/Users/..." khac nhau giua cac may.
        enriched = enriched.withColumn(
            "_source_file",
            F.regexp_replace(F.input_file_name(), r"^.*/landing/", ""),
        )
    return enriched.withColumn(PARTITION_COLUMN, F.lit(ingestion_date))


def read_csv_strings(spark, path: str | Path) -> DataFrame:
    """Doc CSV giu nguyen: moi cot la string, chiu duoc xuong dong trong o co ngoac kep.

    multiLine=True: mot o trong ngoac kep co the chua xuong dong (OpenDengue V1.3
    co 4 dong "ARUNACHAL

PRADESH"). Doc theo dong vat ly se cat ban ghi do
    thanh 2 dong lech cot. escape='"': CSV chuan (va pandas) viet dau ngoac kep
    trong o thanh "" chu khong phai \".

    Args:
        spark: SparkSession.
        path: File hoac glob CSV.

    Returns:
        DataFrame toan cot string.
    """
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(str(path))
    )


def check_existing_table(spark, source: str, partition_column: str) -> None:
    """Chan ghi vao bang Bronze cu co cau truc khong con tuong thich.

    Bang tao boi code truoc 29/9/2026 co the phan vung khac (OpenDengue:
    ingestion_date, nay la release) hoac co cot khong phai string (WHO tu suy
    kieu). Ghi de vao do se loi AnalysisException kho hieu - nen bao ro o day.

    Args:
        spark: SparkSession.
        source: Ten bang Bronze.
        partition_column: Cot phan vung ma code hien tai dung.

    Raises:
        IngestionValidationError: Neu bang da co nhung khac cot phan vung, hoac
            co cot khong phai string.
    """
    from delta.tables import DeltaTable

    target = str(bronze_path(source))
    if not DeltaTable.isDeltaTable(spark, target):
        return
    partitions = list(DeltaTable.forPath(spark, target).detail().first()["partitionColumns"])
    non_string = [f.name for f in spark.read.format("delta").load(target).schema.fields
                  if f.dataType.simpleString() != "string"]
    problems = []
    if partitions != [partition_column]:
        problems.append(f"phan vung theo {partitions}, code can [{partition_column!r}]")
    if non_string:
        problems.append(f"cot khong phai string: {non_string}")
    if problems:
        raise IngestionValidationError(
            f"[{source}] bang Bronze cu khong tuong thich ({'; '.join(problems)}). "
            f"Chuyen {target} sang thu muc backup roi chay lai - xem docs/bronze-fixes.md"
        )


def ingested_rows(spark, source: str, **equals: str) -> int:
    """So dong Bronze khop moi dieu kien cot = gia tri; 0 neu bang chua co.

    Dung cho nguon phat hanh theo phien ban (OpenDengue, HDX, Zenodo): phien
    ban + dau van tay file da nap roi thi bo qua, khong tai lai. "Da nap gi"
    doc nguoc tu chinh bang Delta - khong co file trang thai rieng de lech.

    Args:
        spark: SparkSession.
        source: Ten bang Bronze.
        **equals: Cac cap cot = gia tri, vd release="V1.3", _file_sha="...".

    Returns:
        So dong khop.
    """
    from delta.tables import DeltaTable

    target = str(bronze_path(source))
    if not DeltaTable.isDeltaTable(spark, target):
        return 0
    frame = spark.read.format("delta").load(target)
    if any(column not in frame.columns for column in equals):
        return 0
    for column, value in equals.items():
        frame = frame.where(F.col(column) == value)
    return frame.count()


def write_bronze(
    df: DataFrame,
    source: str,
    partition_value: str,
    partition_column: str = PARTITION_COLUMN,
) -> Path:
    """Ghi DataFrame vao bang Delta Bronze cua mot nguon.

    Idempotency - chon cach GHI DE PHAN VUNG NGAY:
        `replaceWhere` chi thay dung phan vung cua ngay dang chay, cac ngay
        khac khong bi dong den. Chay lai cung mot ngay bao nhieu lan cung ra
        cung ket qua, khong bao gio noi them ban sao.

        Dieu nay doi hoi phan vung cua ngay phai duoc dung lai DAY DU tu
        landing moi lan chay - tuc la doc het cac file raw cua ngay do, chu
        khong chi file vua tai ve. Ca ba nguon deu lam vay.

    Args:
        df: DataFrame da co cot lineage va cot phan vung.
        source: Ten nguon.
        partition_value: Gia tri phan vung can thay the (ngay YYYY-MM-DD, hoac
            ten ban phat hanh voi OpenDengue).
        partition_column: Ten cot phan vung.

    Returns:
        Duong dan bang Delta da ghi.

    Raises:
        IngestionValidationError: Neu bang da co mang cau truc cu (check_existing_table).
    """
    check_existing_table(df.sparkSession, source, partition_column)
    target = bronze_path(source)
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy(partition_column)
        .option("replaceWhere", f"{partition_column} = '{partition_value}'")
        .option("mergeSchema", "true")
        .save(str(target))
    )
    return target

