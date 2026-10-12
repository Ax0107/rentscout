"""
Улучшенный HTTP клиент с подменой TLS-отпечатка, прокси, заголовками и задержками.

Основа — curl_cffi (impersonate): обычный httpx площадки режут по отпечатку.
Интерфейс get/post сохранён (status_code/text/json) — вызывающий код не меняется.
"""
import asyncio
import os
import random
from typing import Optional, Dict, Any

from curl_cffi.requests import AsyncSession

from app.core.config import settings
from app.utils.headers import get_random_headers
from app.utils.proxy import proxy_manager
from app.utils.logger import logger

IMPERSONATE = os.getenv("IMPERSONATE", "chrome")


class EnhancedHTTPClient:
    """
    HTTP клиент для обхода защиты сайтов.

    Особенности:
    - Подмена TLS/JA3-отпечатка (curl_cffi impersonate)
    - Случайные User-Agent и заголовки
    - Прокси с ротацией
    - Задержки между запросами
    """

    def __init__(
        self,
        timeout: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.session: Optional[AsyncSession] = None
        self.last_request_time: float = 0
        self.timeout = timeout
        self.default_headers = headers or {}

    async def __aenter__(self):
        await self.create_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def create_session(self):
        """Создаёт HTTP-сессию с подменой отпечатка и настройками."""
        proxy = None
        if settings.PROXY_ENABLED:
            p = proxy_manager.get_random_proxy()
            if p:
                proxy = p.to_url()
                logger.info(f"Using proxy: {p.host}:{p.port}")

        kwargs: Dict[str, Any] = {
            "impersonate": IMPERSONATE,
            "timeout": self.timeout or settings.REQUEST_TIMEOUT,
            "verify": False,
            "allow_redirects": False,
        }
        if proxy:
            kwargs["proxy"] = proxy
        if self.default_headers:
            kwargs["headers"] = self.default_headers

        self.session = AsyncSession(**kwargs)

    async def _add_random_delay(self):
        """Задержка между запросами."""
        import time
        elapsed = time.time() - self.last_request_time
        if elapsed < settings.MIN_REQUEST_DELAY:
            delay = random.uniform(
                settings.MIN_REQUEST_DELAY - elapsed,
                settings.MAX_REQUEST_DELAY,
            )
            logger.debug(f"Adding delay: {delay:.2f}s")
            await asyncio.sleep(delay)
        self.last_request_time = time.time()

    def _merge_headers(self, headers, use_random_headers):
        merged = dict(self.default_headers)
        if use_random_headers and settings.USE_RANDOM_HEADERS:
            merged.update(get_random_headers())
        if headers:
            merged.update(headers)
        return merged

    async def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        use_random_headers: bool = True,
        **kwargs,
    ):
        """GET с защитой от блокировки."""
        if not self.session:
            await self.create_session()
        await self._add_random_delay()
        request_headers = self._merge_headers(headers, use_random_headers)
        logger.debug(f"GET {url}")
        try:
            return await self.session.get(url, headers=request_headers, params=params, **kwargs)
        except Exception as e:
            logger.error(f"Request error: {e}")
            raise

    async def post(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        use_random_headers: bool = True,
        **kwargs,
    ):
        """POST с защитой от блокировки."""
        if not self.session:
            await self.create_session()
        await self._add_random_delay()
        request_headers = self._merge_headers(headers, use_random_headers)
        logger.debug(f"POST {url}")
        try:
            return await self.session.post(url, headers=request_headers, data=data, json=json, **kwargs)
        except Exception as e:
            logger.error(f"Request error: {e}")
            raise

    async def close(self):
        """Закрывает сессию."""
        if self.session:
            await self.session.close()
            self.session = None
