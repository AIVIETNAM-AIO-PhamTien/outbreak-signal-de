"""HTTP dung chung cho moi nguon: thu lai loi tam thoi, tai file an toan.

Truoc day ca ba nguon goi `requests.get` truc tiep: mot lan timeout la ca lan
chay FAILED, du `retries: 3` da khai bao trong configs/sources.yaml (khong ai doc).
Module nay dung `tenacity` va lay so lan thu lai tu config cua nguon.

Thu lai: loi ket noi, timeout, HTTP 429 va 5xx - loi TAM THOI cua mang/server.
Khong thu lai: 4xx con lai - thu lai 404/403 khong bao gio thanh cong, chi lam
cham viec bao loi.
"""

from pathlib import Path
from typing import Any

import requests
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

USER_AGENT = "OutbreakSignalDE/0.1 (AIO student project)"
CHUNK_BYTES = 1 << 20
# Cho 2, 4, 8... giay giua cac lan, toi da 30 giay.
WAIT = wait_exponential(multiplier=2, max=30)


def is_retryable(error: BaseException) -> bool:
    """Loi nay co dang thu lai khong.

    Args:
        error: Ngoai le vua xay ra.

    Returns:
        True voi loi ket noi, timeout, dut ket noi giua luc tai, HTTP 429 va 5xx.
    """
    # ChunkedEncodingError / ContentDecodingError: server cat ket noi GIUA luc
    # iter_content (IncompleteRead) - khong phai lop con cua ConnectionError.
    transient = (requests.ConnectionError, requests.Timeout,
                 requests.exceptions.ChunkedEncodingError,
                 requests.exceptions.ContentDecodingError)
    if isinstance(error, transient):
        return True
    if isinstance(error, requests.HTTPError) and error.response is not None:
        status = error.response.status_code
        return status == 429 or status >= 500
    return False


def _retrying(retries: int) -> Retrying:
    """Bo dieu khien retry: `retries` lan thu LAI (tong cong retries + 1 lan goi).

    Args:
        retries: So lan thu lai, lay tu config (`retries`).

    Returns:
        Doi tuong tenacity.Retrying; het luot thi nem lai loi goc.
    """
    return Retrying(
        retry=retry_if_exception(is_retryable),
        stop=stop_after_attempt(retries + 1),
        wait=WAIT,
        reraise=True,
    )


def _raise_for_status(response: requests.Response) -> None:
    """Nhu raise_for_status(), nhung noi ro khi het rate limit cua GitHub.

    GitHub tra 403 (khong phai 429) khi het 60 request/gio khong token - de
    nham voi loi quyen truy cap.

    Args:
        response: Response vua nhan.

    Raises:
        RuntimeError: Neu het rate limit GitHub.
        requests.HTTPError: Cac loi 4xx/5xx khac.
    """
    if response.status_code == 403 and response.headers.get("X-RateLimit-Remaining") == "0":
        raise RuntimeError(
            "het rate limit GitHub API (60 request/gio khi khong co token), "
            f"reset luc epoch {response.headers.get('X-RateLimit-Reset')}"
        )
    response.raise_for_status()


def get(url: str, retries: int, timeout: float, **kwargs: Any) -> requests.Response:
    """GET co thu lai loi tam thoi.

    Args:
        url: URL.
        retries: So lan thu lai.
        timeout: Timeout moi lan goi (giay).
        **kwargs: Truyen tiep cho requests.get (params, headers...).

    Returns:
        Response thanh cong (2xx).

    Raises:
        requests.HTTPError / requests.RequestException: Khi het luot thu lai
            hoac gap loi khong dang thu lai.
    """
    headers = {"User-Agent": USER_AGENT, **kwargs.pop("headers", {})}
    for attempt in _retrying(retries):
        with attempt:
            response = requests.get(url, headers=headers, timeout=timeout, **kwargs)
            _raise_for_status(response)
    return response


def download(url: str, dest: Path, retries: int, timeout: float) -> Path:
    """Tai file theo tung khuc ra `<dest>.part`, xong moi doi ten thanh `dest`.

    Khong nap ca file vao RAM; mot lan tai do dang (mat mang, bi kill) khong bao
    gio de lai file trong giong file hoan chinh.

    Args:
        url: URL tai truc tiep.
        dest: File dich.
        retries: So lan thu lai.
        timeout: Timeout moi lan cho du lieu (giay).

    Returns:
        Duong dan file da tai.
    """
    part = dest.with_name(dest.name + ".part")
    for attempt in _retrying(retries):
        with attempt:
            with requests.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=timeout, stream=True
            ) as response:
                _raise_for_status(response)
                with open(part, "wb") as handle:
                    for chunk in response.iter_content(CHUNK_BYTES):
                        handle.write(chunk)
    part.replace(dest)
    return dest
