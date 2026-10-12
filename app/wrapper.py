"""
Микросервис внешних объявлений для AIOPA (FastAPI, :8001).

Единственный источник истины по добыче — `app/parsers/*` (Cian/Yandex/
Domclick/Avito). Своих парсеров и браузерного рендера здесь больше нет:
раньше воркер тащил собственный Playwright-забор, который дублировал
парсеры и расходился с ними по селекторам.

Контракт ответа /search сохранён ({items, per_source, source_links, ...}),
чтобы AIOPA (App\\Services\\External\\ExternalPropertySearchService) не менялся.
"""
import asyncio
import hashlib
import json
import logging
import os
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query

from app.parsers.avito.parser import AvitoParser
from app.parsers.cian.parser import CianParser
from app.parsers.domclick.parser import DomclickParser
from app.parsers.yandex_realty.parser import YandexRealtyParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("wrapper")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_TTL = 600      # 10 min — выдача поиска
ITEM_TTL  = 86400    # 24h — отдельные карточки

# Ключ источника → (человеческое имя, класс парсера, корневой URL площадки)
SOURCES: Dict[str, tuple] = {
    "cian":     ("Циан",                 CianParser,        "https://www.cian.ru/"),
    "yandex":   ("Яндекс Недвижимость",  YandexRealtyParser, "https://realty.yandex.ru/"),
    "domclick": ("Домклик",              DomclickParser,    "https://domclick.ru/"),
    "avito":    ("Авито",                AvitoParser,       "https://www.avito.ru/"),
}


def make_item_id(source: str, url: str) -> str:
    return hashlib.md5(f"{source}|{url}".encode()).hexdigest()[:16]


def format_price(n) -> Optional[str]:
    try:
        n = int(n)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    if n >= 1_000_000:
        v = n / 1_000_000
        s = f"{v:.1f}".rstrip("0").rstrip(".")
        return f"{s} млн ₽"
    if n >= 1_000:
        return f"{n // 1000} тыс ₽"
    return f"{n} ₽"


def _params(filters: Dict[str, Any]) -> Dict[str, Any]:
    """Флаги воркера → параметры парсеров (operation/rooms/price/type).

    Ключи с None НЕ кладём: парсеры проверяют наличие ключа, а не его значение,
    и падают на `None.lower()`.
    """
    def _int(v):
        try:
            return int(str(v).strip()) if v not in (None, "", "null") else None
        except (TypeError, ValueError):
            return None

    raw = {
        "operation": filters.get("operation") or "rent_long",
        "rooms": _int(filters.get("rooms")),
        "price_min": _int(filters.get("price_min")),
        "price_max": _int(filters.get("price_max")),
        "type": filters.get("type") or None,
        "search": filters.get("search") or None,
    }
    return {k: v for k, v in raw.items() if v is not None}


def _to_item(p: Any, source: str, operation: str, property_type: str) -> Optional[Dict[str, Any]]:
    """PropertyCreate (app/parsers) → item-формат, который ждёт AIOPA."""
    url = (getattr(p, "link", None) or "").strip()
    if not url:
        return None

    price_raw = int(getattr(p, "price", 0) or 0)
    area = getattr(p, "area", None)
    price_per_m = getattr(p, "price_per_sqm", None)
    if price_per_m is None and price_raw and area:
        try:
            price_per_m = round(price_raw / float(area), 1)
        except (TypeError, ZeroDivisionError):
            price_per_m = None

    photos = list(getattr(p, "photos", None) or [])
    op_type = "rent" if "rent" in (operation or "") else "sale"

    return {
        "source":         source,
        "source_label":   SOURCES[source][0],
        "title":          getattr(p, "title", None) or "Объявление",
        "subtitle":       None,
        "operation_type": op_type,
        "property_type":  property_type,
        "price":          format_price(price_raw),
        "price_raw":      price_raw,
        "price_per_m":    price_per_m,
        "city":           getattr(p, "city", None),
        "address":        getattr(p, "address", None),
        "metro":          None,
        "rooms":          getattr(p, "rooms", None),
        "area":           area,
        "floor":          getattr(p, "floor", None),
        "total_floors":   getattr(p, "total_floors", None),
        "lat":            getattr(p, "latitude", None),
        "lon":            getattr(p, "longitude", None),
        "description":    getattr(p, "description", None),
        "url":            url,
        "image":          photos[0] if photos else None,
        "thumbs":         photos[:8],
    }


async def _run_source(key: str, location: str, filters: Dict[str, Any], timeout: float = 90) -> List[Dict[str, Any]]:
    """Прогнать один парсер и смапить результат в items."""
    label, parser_cls, _ = SOURCES[key]
    params = _params(filters)
    property_type = filters.get("type") or "apartment"
    try:
        parsed = await asyncio.wait_for(parser_cls().parse(location, params), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"[{key}] timeout {timeout}s")
        return []
    except Exception as e:
        logger.warning(f"[{key}] parse failed: {e}")
        return []

    items = []
    seen = set()
    for p in parsed or []:
        it = _to_item(p, key, params["operation"], property_type)
        if not it or it["url"] in seen:
            continue
        seen.add(it["url"])
        items.append(it)
    return items


# ============================ FastAPI app ============================

app = FastAPI(title="AIOPA External Scraper", version="0.3.0")

_redis: Optional[aioredis.Redis] = None


@app.on_event("startup")
async def on_startup():
    global _redis
    try:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        await _redis.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        _redis = None


@app.get("/health")
async def health():
    return {"status": "ok", "redis": _redis is not None, "engine": "app/parsers"}


@app.get("/sources")
async def sources():
    return [{"key": k, "label": v[0]} for k, v in SOURCES.items()]


@app.get("/search")
async def search(
    search:    Optional[str] = Query(None),
    city:      Optional[str] = Query(None),
    type:      Optional[str] = Query(None),
    operation: Optional[str] = Query(None),
    rooms:     Optional[str] = Query(None),
    price_min: Optional[str] = Query(None),
    price_max: Optional[str] = Query(None),
    page:      int = Query(1, ge=1),
    per_page:  int = Query(100, ge=1, le=100),
    pages:     int = Query(3, ge=1, le=10),
):
    filters = {k: v for k, v in {
        "search": search, "city": city, "type": type, "operation": operation,
        "rooms": rooms, "price_min": price_min, "price_max": price_max,
    }.items() if v}

    location = filters.get("city") or "Москва"

    cache_key = "aiopa:ext:" + json.dumps(
        {**filters, "_city": location}, sort_keys=True, ensure_ascii=False
    )
    if _redis:
        try:
            cached = await _redis.get(cache_key)
            if cached:
                logger.info(f"cache hit: {cache_key}")
                return json.loads(cached)
        except Exception as e:
            logger.debug(f"cache read err: {e}")

    # Парсеры сами ходят по страницам (CIAN_PAGES/YANDEX_PAGES/DOMCLICK_PAGES),
    # поэтому воркер дёргает каждый источник один раз и параллельно.
    results = await asyncio.gather(*(_run_source(k, location, filters) for k in SOURCES))

    merged: List[Dict[str, Any]] = []
    per_source: Dict[str, int] = {}
    for key, items in zip(SOURCES.keys(), results):
        for it in items:
            it["id"] = make_item_id(it.get("source", key), it.get("url", ""))
        per_source[key] = len(items)
        merged.extend(items)

    source_links = [{"key": k, "label": v[0], "url": v[2]} for k, v in SOURCES.items()]
    payload = {
        "items": merged[:per_page],
        "per_source": per_source,
        "total": len(merged),
        "page": page,
        "per_page": per_page,
        "source_links": source_links,
        "filters": filters,
    }

    if _redis and merged:
        try:
            pipe = _redis.pipeline()
            for it in merged:
                if it.get("id"):
                    pipe.setex(f"aiopa:ext_item:{it['id']}", ITEM_TTL, json.dumps(it, ensure_ascii=False))
            await pipe.execute()
        except Exception as e:
            logger.debug(f"per-item cache write err: {e}")
        try:
            await _redis.setex(cache_key, CACHE_TTL, json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            logger.debug(f"cache write err: {e}")

    return payload


@app.get("/item/{item_id}")
async def get_item(item_id: str):
    if not _redis:
        raise HTTPException(503, "cache unavailable")
    data = await _redis.get(f"aiopa:ext_item:{item_id}")
    if not data:
        raise HTTPException(404, "item not found")
    return json.loads(data)


@app.get("/detail/{item_id}")
async def get_detail(item_id: str):
    """Карточка из кэша.

    Отдельного detail-скрейпа здесь больше нет: данные по объекту целиком
    приходят из app/parsers при поиске, а браузерный обход карточек убран.
    """
    if not _redis:
        raise HTTPException(503, "cache unavailable")
    card_data = await _redis.get(f"aiopa:ext_item:{item_id}")
    if not card_data:
        raise HTTPException(404, "item not found")
    return json.loads(card_data)
