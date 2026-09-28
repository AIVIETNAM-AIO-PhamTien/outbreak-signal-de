"""
Tien ich dung chung cho toan bo pipeline ingestion.

Moi thu lien quan den Spark deu gom vao day, de cac file ingest_*.py cua tung
nguon chi con logic don gian cua rieng nguon do.
"""

import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_ROOT = PROJECT_ROOT / "data" / "landing"
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"

# Spark 3.5 chi chay on dinh tren cac ban Java nay.
SUPPORTED_JAVA_MAJORS = {8, 11, 17, 21}


def _detect_java_major() -> int | None:
    """Tra ve major version cua Java ma Spark se dung, hoac None neu khong doc duoc."""
    java_home = os.environ.get("JAVA_HOME")
    java_cmd = str(Path(java_home) / "bin" / "java") if java_home else "java"
    try:
        out = subprocess.run(
            [java_cmd, "-version"], capture_output=True, text=True, timeout=30
        ).stderr
    except (OSError, subprocess.SubprocessError):
        return None

    m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
    if not m:
        return None
    major, minor = int(m.group(1)), m.group(2)
    # Java 8 bao version la "1.8.0_xxx" -> major that su nam o phan minor.
    return int(minor) if major == 1 and minor else major


def check_java() -> None:
    """Dung som voi thong bao ro rang neu Java khong tuong thich Spark."""
    major = _detect_java_major()
    if major is None:
        print("[WARN] Khong doc duoc phien ban Java. Neu Spark loi, kiem tra lai JAVA_HOME.")
        return
    if major not in SUPPORTED_JAVA_MAJORS:
        print(
            f"[ERROR] Dang dung Java {major}, nhung Spark 3.5 chi ho tro Java "
            f"{sorted(SUPPORTED_JAVA_MAJORS)}.\n"
            f"        Cach sua: cai JDK 17 roi tro JAVA_HOME vao no truoc khi chay, vi du:\n"
            f'          PowerShell: $env:JAVA_HOME = "C:\\Program Files\\Microsoft\\jdk-17.x.x-hotspot"\n'
            f"        (Xem huong dan day du trong README.md)",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"[OK] Java {major} - tuong thich voi Spark 3.5.")


def get_spark(app_name: str) -> SparkSession:
    """Tao SparkSession chay o che do local (khong can cluster)."""
    check_java()
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # dung toan bo CPU cua may, khong can cai cluster
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # bot log rac cho de doc output
    return spark


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def landing_dir(source: str, ingest_date: str) -> Path:
    """Thu muc chua file raw tai ve nguyen trang, truoc khi Spark doc."""
    path = LANDING_ROOT / source / ingest_date
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_bronze(df: DataFrame, source: str, ingest_date: str) -> int:
    """
    Ghi DataFrame vao Bronze layer duoi dang Parquet.

    Quy tac Bronze: GIU NGUYEN TRANG du lieu - khong doi ten cot, khong loc,
    khong dedup, khong ep kieu. Chi them 3 cot metadata de truy vet nguon goc.
    Moi viec lam sach de dan Silver layer (tuan sau) xu ly.

    Moi lan chay se ghi de dung thu muc cua ngay do, nen chay lai nhieu lan
    trong cung mot ngay van cho ket qua dung (khong bi nhan doi du lieu).
    """
    enriched = (
        df.withColumn("_source", F.lit(source))
        .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc).isoformat()))
        # input_file_name() cho biet dong nay den tu file nao - huu ich khi mot
        # ngay co nhieu lan fetch (nguon tin tuc chay moi 15-60 phut).
        .withColumn("_source_file", F.input_file_name())
    )

    out_path = BRONZE_ROOT / source / f"ingest_date={ingest_date}"
    enriched.write.mode("overwrite").parquet(str(out_path))

    count = enriched.count()
    print(f"      -> ghi {count:,} dong vao {out_path}")
    return count
