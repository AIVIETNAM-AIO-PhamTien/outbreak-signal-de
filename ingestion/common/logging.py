"""Cau hinh log dung chung cho moi job ingestion.

Truoc day cac job dung print() nen khong co muc do log, khong co timestamp,
va khong luu lai duoc. Module nay thay the bang `logging` cua thu vien chuan:
in ra man hinh va ghi dong thoi vao logs/ingestion_<ngay>.log.

Luu y ve ten file: module nay ten `logging.py` nhung ben trong van `import
logging` ra duoc thu vien chuan, vi Python 3 mac dinh dung absolute import -
`import logging` tim o top-level chu khong tim trong package hien tai.
"""

import logging
import sys

from ingestion.common.paths import LOG_ROOT, today_str

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DATE_FORMAT = "%H:%M:%S"

_configured = False


def setup_logging(level: int = logging.INFO) -> None:
    """Gan handler cho root logger. Goi nhieu lan cung chi chay mot lan.

    Ghi ra hai noi: stdout de nhin truc tiep luc chay, va file theo ngay de
    con doi chieu lai khi mot job chay theo lich that bai luc khong ai ngoi xem.

    Args:
        level: Muc log toi thieu, mac dinh INFO.
    """
    global _configured
    if _configured:
        return

    LOG_ROOT.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)

    logfile = logging.FileHandler(
        LOG_ROOT / f"ingestion_{today_str()}.log", encoding="utf-8"
    )
    logfile.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(logfile)

    # Spark log qua py4j rat on, chi giu canh bao tro len.
    logging.getLogger("py4j").setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Tra ve logger da cau hinh san cho mot module hoac mot nguon.

    Args:
        name: Ten logger, thuong la ten nguon ("opendengue") de doc log biet
            ngay dong do cua job nao.

    Returns:
        Logger dung duoc ngay.
    """
    setup_logging()
    return logging.getLogger(name)
