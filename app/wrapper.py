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
import time
from itertools import zip_longest
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query

from app.parsers.cian.parser import CianParser
from app.parsers.domclick.parser import DomclickParser
from app.parsers.yandex_realty.parser import YandexRealtyParser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("wrapper")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CACHE_TTL = 600      # 10 min — выдача поиска
ITEM_TTL  = 86400    # 24h — отдельные карточки

# Ключ источника → (человеческое имя, класс парсера, корневой URL площадки)
# avito убран: с него жёсткий IP-бан (403/429), из выдачи он приносит 0 и
# только жжёт время и дёргает предохранитель. Вернём, когда будет чистый IP.
SOURCES: Dict[str, tuple] = {
    "cian":     ("Циан",                 CianParser,        "https://www.cian.ru/"),
    "yandex":   ("Яндекс Недвижимость",  YandexRealtyParser, "https://realty.yandex.ru/"),
    "domclick": ("Домклик",              DomclickParser,    "https://domclick.ru/"),
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
        # Сколько страниц тянуть у площадок: сбор просит много, диалог — мало.
        "pages": _int(filters.get("pages")),
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
    # НЕ схлопываем до rent/sale: аренда долгосрочная и посуточная — разные
    # типы (в базе sell|rent_long|rent_short), терять разницу нельзя.
    op_norm = operation if operation in ("sell", "rent_long", "rent_short") else (
        "rent_long" if "rent" in (operation or "") else "sell"
    )

    return {
        "source":         source,
        "source_label":   SOURCES[source][0],
        "title":          getattr(p, "title", None) or "Объявление",
        "subtitle":       None,
        "operation":      op_norm,
        "operation_type": op_norm,
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


def _num(v):
    try:
        return float(str(v).replace(" ", "").replace("\u00a0", ""))
    except (TypeError, ValueError):
        return None


def _truthy(v) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on", "да")


def _apply_filters(items: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Применить все фильтры к готовым items.

    Площадки большинство слотов в запросе игнорируют, поэтому фильтруем здесь —
    в микросервисе, а не на стороне AIOPA: rentscout — единственное место, где
    известны и данные, и фильтры. Нет данных в элементе — не отбрасываем
    (потерять годный объект хуже, чем показать лишний).
    """
    pmin, pmax = _num(filters.get("price_min")), _num(filters.get("price_max"))
    rooms = _num(filters.get("rooms"))
    amin, amax = _num(filters.get("area_min")), _num(filters.get("area_max"))
    fmin, fmax = _num(filters.get("floor_min")), _num(filters.get("floor_max"))
    nff = _truthy(filters.get("not_first_floor"))
    lastf = _truthy(filters.get("only_last_floor"))

    out = []
    for it in items:
        price = it.get("price_raw") or 0
        if price:
            if pmin and price < pmin:
                continue
            if pmax and price > pmax:
                continue

        r = it.get("rooms")
        if rooms and r is not None:
            r = int(r)
            if rooms >= 4:
                if r < 4:
                    continue
            elif r != int(rooms):
                continue

        a = _num(it.get("area"))
        if a is not None:
            if amin and a < amin:
                continue
            if amax and a > amax:
                continue

        fl = _num(it.get("floor"))
        if fl is not None:
            if fmin and fl < fmin:
                continue
            if fmax and fl > fmax:
                continue
            if nff and fl == 1:
                continue
        if lastf:
            tf = _num(it.get("total_floors"))
            if fl is not None and tf is not None and fl != tf:
                continue

        out.append(it)
    return out


async def _run_source(key: str, location: str, filters: Dict[str, Any], timeout: float = 300) -> List[Dict[str, Any]]:
    """Прогнать один парсер, смапить в items и применить фильтры.

    timeout большой: глубокий сбор (pages=100) у Циана идёт дольше, и раньше
    его срезало на 90с — в выдаче оставались только Яндекс и Домклик.
    """
    if timeout is None:
        timeout = float(os.getenv("RUN_SOURCE_TIMEOUT", "300"))
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
    return _apply_filters(items, filters)


# ============================ FastAPI app ============================

app = FastAPI(title="AIOPA External Scraper", version="0.3.0")

_redis: Optional[aioredis.Redis] = None


# ---- Кэш: Redis, а без него — память процесса ----
# Раньше /item и /detail при отсутствии Redis отдавали 503, и страница
# объекта в AIOPA падала в 404: карточка жила только в кэше. Теперь есть
# резервный кэш в памяти воркера (один процесс) — страница открывается и
# без Redis.
_mem: Dict[str, tuple] = {}


def _mem_set(key: str, val: str, ttl: int) -> None:
    if len(_mem) > 5000:
        now = time.time()
        for k in [k for k, (e, _) in _mem.items() if e < now]:
            _mem.pop(k, None)
        if len(_mem) > 5000:
            _mem.clear()
    _mem[key] = (time.time() + ttl, val)


def _mem_get(key: str) -> Optional[str]:
    v = _mem.get(key)
    if not v:
        return None
    exp, val = v
    if exp < time.time():
        _mem.pop(key, None)
        return None
    return val


async def cache_get(key: str) -> Optional[str]:
    if _redis:
        try:
            return await _redis.get(key)
        except Exception as e:
            logger.debug(f"redis get err: {e}")
    return _mem_get(key)


async def cache_set(key: str, val: str, ttl: int) -> None:
    _mem_set(key, val, ttl)
    if _redis:
        try:
            await _redis.setex(key, ttl, val)
        except Exception as e:
            logger.debug(f"redis set err: {e}")


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
    area_min:  Optional[str] = Query(None),
    area_max:  Optional[str] = Query(None),
    floor_min: Optional[str] = Query(None),
    floor_max: Optional[str] = Query(None),
    not_first_floor:  Optional[str] = Query(None),
    only_last_floor:  Optional[str] = Query(None),
    page:      int = Query(1, ge=1),
    per_page:  int = Query(200, ge=1, le=300),
    pages:     Optional[int] = Query(None, ge=1, le=200),
):
    filters = {k: v for k, v in {
        "search": search, "city": city, "type": type, "operation": operation,
        "rooms": rooms, "price_min": price_min, "price_max": price_max,
        "area_min": area_min, "area_max": area_max,
        "floor_min": floor_min, "floor_max": floor_max,
        "not_first_floor": not_first_floor, "only_last_floor": only_last_floor,
        "pages": pages,
    }.items() if v not in (None, "")}

    location = filters.get("city") or "Москва"

    cache_key = "aiopa:ext:" + json.dumps(
        {**filters, "_city": location}, sort_keys=True, ensure_ascii=False
    )
    cached = await cache_get(cache_key)
    if cached:
        logger.info(f"cache hit: {cache_key}")
        return json.loads(cached)

    # Парсеры сами ходят по страницам (CIAN_PAGES/YANDEX_PAGES/DOMCLICK_PAGES),
    # поэтому воркер дёргает каждый источник один раз и параллельно.
    results = await asyncio.gather(*(_run_source(k, location, filters) for k in SOURCES))

    per_source: Dict[str, int] = {}
    lists: List[List[Dict[str, Any]]] = []
    for key, items in zip(SOURCES.keys(), results):
        for it in items:
            it["id"] = make_item_id(it.get("source", key), it.get("url", ""))
        per_source[key] = len(items)
        lists.append(items)

    # Чередуем площадки. Источники склеены последовательно (cian→yandex→domclick),
    # и срез merged[:per_page] заполнялся целиком первым — Яндекс и Домклик в
    # выдачу не попадали, хотя объявления у них есть.
    merged: List[Dict[str, Any]] = []
    for group in zip_longest(*lists):
        for x in group:
            if x is not None:
                merged.append(x)

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

    if merged:
        for it in merged:
            if it.get("id"):
                await cache_set(f"aiopa:ext_item:{it['id']}", json.dumps(it, ensure_ascii=False), ITEM_TTL)
        await cache_set(cache_key, json.dumps(payload, ensure_ascii=False), CACHE_TTL)

    return payload


@app.get("/item/{item_id}")
async def get_item(item_id: str):
    data = await cache_get(f"aiopa:ext_item:{item_id}")
    if not data:
        raise HTTPException(404, "item not found")
    return json.loads(data)


@app.get("/detail/{item_id}")
async def get_detail(item_id: str):
    """Карточка из кэша.

    Отдельного detail-скрейпа здесь больше нет: данные по объекту целиком
    приходят из app/parsers при поиске, а браузерный обход карточек убран.
    """
    card_data = await cache_get(f"aiopa:ext_item:{item_id}")
    if not card_data:
        raise HTTPException(404, "item not found")
    return json.loads(card_data)
