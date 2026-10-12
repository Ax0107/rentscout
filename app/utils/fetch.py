"""Добыча HTML для парсеров — без браузера.

Единственный путь — curl_cffi с подменой TLS/JA3-отпечатка. Площадки режут
обычный httpx по отпечатку (не по IP), impersonate его подменяет. Браузерных
фолбэков нет намеренно.

Заголовки важны не меньше отпечатка: часть площадок (Авито) смотрит на
sec-ch-ua / sec-fetch-* / accept / referer. Поэтому headers можно переопределить.
"""
import asyncio
import logging
import os
import random
from typing import Dict, Optional

from curl_cffi import requests as creq

logger = logging.getLogger(__name__)

IMPERSONATE = os.getenv("IMPERSONATE", "chrome")
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "30"))
MIN_BYTES = int(os.getenv("FETCH_MIN_BYTES", "5000"))

DEFAULT_HEADERS: Dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


def _sync_get(url: str, impersonate: str, headers: Dict[str, str], timeout: int):
    # verify=False: у части площадок битая цепочка сертификатов; страницы публичные,
    # read-only — проверку сертификата для парсинга отключаем.
    return creq.get(url, impersonate=impersonate, headers=headers, timeout=timeout, verify=False)


async def fetch_html(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    impersonate: Optional[str] = None,
    timeout: int = FETCH_TIMEOUT,
    min_bytes: int = MIN_BYTES,
) -> Optional[str]:
    """Вернуть HTML страницы или None, если не удалось/страница пустая."""
    if not url:
        return None

    merged = dict(DEFAULT_HEADERS)
    if headers:
        merged.update(headers)

    attempts = int(os.getenv("FETCH_ATTEMPTS", "3"))
    last_err = ""
    for attempt in range(attempts):
        try:
            r = await asyncio.to_thread(
                _sync_get, url, impersonate or IMPERSONATE, merged, timeout
            )
            text = r.text or ""
            if r.status_code == 200 and len(text) >= min_bytes:
                return text
            last_err = f"http={r.status_code} bytes={len(text)}"
        except Exception as e:
            last_err = str(e)
        if attempt < attempts - 1:
            # Отбившиеся от рейт-лимита страницы часто проходят со второй попытки.
            await asyncio.sleep(random.uniform(0.6, 1.8))

    logger.warning("fetch failed %s: %s", url, last_err)
    return None


async def fetch_many(
    urls: list,
    *,
    headers: Optional[Dict[str, str]] = None,
    impersonate: Optional[str] = None,
    timeout: int = FETCH_TIMEOUT,
    concurrency: int = 6,
) -> list:
    """Параллельно забрать список URL (с ограничением одновременности)."""
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(u: str) -> Optional[str]:
        async with sem:
            return await fetch_html(u, headers=headers, impersonate=impersonate, timeout=timeout)

    return await asyncio.gather(*(one(u) for u in urls))
