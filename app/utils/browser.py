"""Браузерный рендер HTML — ТОЛЬКО для площадок с JS-защитой (сейчас: Домклик/Qrator).

Основной путь добычи — curl_cffi (app/utils/fetch.py). Здесь browser нужен
точечно: Qrator на Домклике отдаёт 401 без выполнения JS-челленджа, а чистого
API-пути нет. Браузер выполняет челлендж сам.

Управление окружением:
  RENDER_ENABLED=0        — выключить (тогда Домклик падает на httpx)
  RENDER_HEADFUL=1        — окно (по умолчанию 1: Qrator агрессивнее к headless)
  RENDER_CHANNEL=chrome   — chrome | chromium | msedge
  RENDER_USER_DATA_DIR    — постоянный профиль
  RENDER_TIMEOUT_MS / RENDER_WAIT_MS / RENDER_CONCURRENCY
"""
import asyncio
import logging
import os
import random
import re
from typing import Optional

logger = logging.getLogger(__name__)

RENDER_ENABLED = os.getenv("RENDER_ENABLED", "1") != "0"
RENDER_CHANNEL = os.getenv("RENDER_CHANNEL", "chrome")
RENDER_HEADFUL = os.getenv("RENDER_HEADFUL", "1") == "1"
RENDER_USER_DATA_DIR = os.getenv("RENDER_USER_DATA_DIR", "")
RENDER_TIMEOUT_MS = int(os.getenv("RENDER_TIMEOUT_MS", "45000"))
RENDER_WAIT_MS = int(os.getenv("RENDER_WAIT_MS", "5000"))

_sem = asyncio.Semaphore(int(os.getenv("RENDER_CONCURRENCY", "2")))
_pw = None
_browser = None
_persistent = None
_lock = asyncio.Lock()

_STEALTH_JS = """
Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
window.chrome=window.chrome||{runtime:{}};
Object.defineProperty(navigator,'languages',{get:()=>['ru-RU','ru','en-US','en']});
Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});
Object.defineProperty(navigator,'hardwareConcurrency',{get:()=>8});
"""

_LAUNCH_ARGS = ["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]


async def _launch():
    global _pw, _browser, _persistent
    # patchright — пропатченный Playwright: проходит детект автоматизации (Qrator
    # на Домклике). Если его нет — обычный playwright.
    try:
        from patchright.async_api import async_playwright
    except ImportError:
        from playwright.async_api import async_playwright

    _pw = await async_playwright().start()

    if RENDER_USER_DATA_DIR:
        _persistent = await _pw.chromium.launch_persistent_context(
            RENDER_USER_DATA_DIR,
            headless=not RENDER_HEADFUL,
            channel=RENDER_CHANNEL,
            args=_LAUNCH_ARGS,
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            viewport={"width": 1440, "height": 900},
        )
        return

    try:
        _browser = await _pw.chromium.launch(channel=RENDER_CHANNEL, headless=not RENDER_HEADFUL, args=_LAUNCH_ARGS)
    except Exception as e:
        logger.warning("render: channel=%s недоступен (%s); беру bundled chromium", RENDER_CHANNEL, e)
        _browser = await _pw.chromium.launch(headless=not RENDER_HEADFUL, args=_LAUNCH_ARGS)


async def _get_ctx():
    global _browser, _persistent, _pw
    async with _lock:
        if RENDER_USER_DATA_DIR:
            if _persistent is None:
                await _launch()
            return _persistent, True
        if _browser is None or not _browser.is_connected():
            _browser = None
            await _launch()
        ctx = await _browser.new_context(
            locale="ru-RU", timezone_id="Europe/Moscow", viewport={"width": 1440, "height": 900}
        )
        return ctx, False


async def _humanize(page) -> None:
    """Имитация живого пользователя: мышь по кривой, скролл, случайные паузы.

    Qrator и подобные смотрят не только на отпечаток, но и на поведение:
    мгновенный page.content() без движений/скролла выглядит как бот.
    """
    try:
        w, h = 1440, 900
        x, y = w / 2, h / 2
        for _ in range(random.randint(4, 8)):
            x2 = max(20, min(w - 20, x + random.uniform(-260, 260)))
            y2 = max(20, min(h - 20, y + random.uniform(-160, 160)))
            await page.mouse.move(x2, y2, steps=random.randint(8, 22))
            x, y = x2, y2
            await page.wait_for_timeout(random.randint(120, 480))
        for _ in range(random.randint(1, 3)):
            await page.mouse.wheel(0, random.randint(250, 900))
            await page.wait_for_timeout(random.randint(300, 900))
    except Exception:
        pass


async def fetch_rendered(url: str) -> Optional[str]:
    """Вернуть отрендеренный HTML (после выполнения JS) или None."""
    if not RENDER_ENABLED or not url:
        return None

    async with _sem:
        ctx = None
        try:
            ctx, persistent = await _get_ctx()
            page = await ctx.new_page()
            # ВАЖНО: не добавляем ручной stealth-скрипт. patchright сам патчит
            # движок, а ручные переопределения (navigator.webdriver и пр.) Qrator
            # как раз детектит — с ними челлендж не проходит.
            await page.goto(url, wait_until="domcontentloaded", timeout=RENDER_TIMEOUT_MS)
            await _humanize(page)
            await page.wait_for_timeout(RENDER_WAIT_MS)
            content = await page.content()

            # Qrator (и подобные) отдают челлендж-страницу, пока JS не решит
            # задачу и не перезагрузит страницу. Ждём и при необходимости
            # перезагружаем, пока не получим настоящую страницу.
            for _ in range(4):
                if "__qrator" not in content and not re.search(r"<title[^>]*>\s*(401|403)", content):
                    break
                await page.wait_for_timeout(random.randint(2500, 4000))
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=RENDER_TIMEOUT_MS)
                except Exception:
                    pass
                await page.wait_for_timeout(RENDER_WAIT_MS)
                content = await page.content()
            return content
        except Exception as e:
            logger.warning("render failed %s: %s", url, e)
            return None
        finally:
            if ctx is not None:
                try:
                    if RENDER_USER_DATA_DIR:
                        for pg in ctx.pages:
                            await pg.close()
                    else:
                        await ctx.close()
                except Exception:
                    pass
