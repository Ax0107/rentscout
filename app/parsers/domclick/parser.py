import asyncio
import httpx
import json
import logging
import os
import re
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.models.schemas import PropertyCreate
from app.parsers.base_parser import BaseParser, metrics_collector_decorator
from app.services.advanced_cache import cached_parser
from app.utils.parser_errors import (
    ParserErrorHandler,
    RateLimitError,
    TimeoutError as ParserTimeoutError,
    NetworkError,
    ParsingError,
    ErrorClassifier,
)
from app.utils.retry import retry
from app.utils.ratelimiter import rate_limiter
from app.utils.circuit_breaker import get_circuit_breaker, CircuitBreakerOpen

logger = logging.getLogger(__name__)

# Официальный BFF-search API Домклика: открыт (вне Qrator), пагинация offset/limit.
DOMCLICK_API = "https://bff-search-web.domclick.ru/api/offers/v1"
DOMCLICK_PAGE_LIMIT = int(os.getenv("DOMCLICK_PAGE_LIMIT", "30"))
DOMCLICK_PAGES = int(os.getenv("DOMCLICK_PAGES", "30"))
DOMCLICK_CONCURRENCY = int(os.getenv("DOMCLICK_CONCURRENCY", "6"))


class DomclickParser(BaseParser):
    BASE_URL = "https://domclick.ru"

    # guid региона для API (Москва проверен). Остальные города — добавлять сюда.
    CITY_GUIDS = {
        "москва": "1d1463ae-c80f-4d19-9331-a1b68a85b553",
        "moscow": "1d1463ae-c80f-4d19-9331-a1b68a85b553",
    }

    def __init__(self):
        super().__init__()
        self.name = "DomclickParser"

    @cached_parser(expire=600, source="domclick")  # Кеш на 10 минут
    @retry(max_attempts=3, initial_delay=1.0, max_delay=10.0)
    @metrics_collector_decorator
    async def parse(self, location: str, params: Dict[str, Any] = None) -> List[PropertyCreate]:
        # Проверяем circuit breaker перед попыткой
        circuit_breaker = get_circuit_breaker("domclick")

        try:
            return await circuit_breaker.call_async(self._parse_internal, location, params)
        except CircuitBreakerOpen as e:
            logger.warning(f"Domclick parser circuit is open: {e}")
            raise NetworkError(f"Domclick service temporarily unavailable: {e}")

    async def _parse_internal(self, location: str, params: Dict[str, Any]) -> List[PropertyCreate]:
        """Внутренний метод парсинга."""
        # Preprocess params
        processed_params = await self.preprocess_params(params)

        op = (processed_params or {}).get("operation", "rent_long")
        deal = "sale" if op == "sell" else "rent"
        guid = self.CITY_GUIDS.get((location or "").strip().lower())

        await rate_limiter.acquire("domclick")

        try:
            # 1) Лёгкий путь — BFF-search API Домклика (без браузера, с пагинацией).
            if guid:
                items = await self._fetch_via_api(guid, deal, processed_params)
                if items:
                    return await self.postprocess_results(items)

            # 2) Фолбэк — браузер (Qrator) + разбор window.__SSR_STATE__.
            url = f"{self.BASE_URL}/search?deal_type={deal}&offer_type=flat&offer_type=layout&category=living"
            from app.utils.browser import fetch_rendered

            html = await fetch_rendered(url)
            if html:
                return await self.postprocess_results(self._parse_html(html))

            # 3) Последний фолбэк — обычный HTTP (Qrator отдаст 401, но пусть будет).
            async with httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT) as client:
                response = await client.get(url)
                response.raise_for_status()
                results = self._parse_html(response.text)
                return await self.postprocess_results(results)
        except asyncio.TimeoutError as e:
            parser_error = ParserTimeoutError(f"Timeout while fetching {url}: {e}")
            ParserErrorHandler.log_error(parser_error, context="DomclickParser._parse_internal")
            raise parser_error
        except httpx.TimeoutException as e:
            parser_error = ParserTimeoutError(f"HTTP timeout: {e}")
            ParserErrorHandler.log_error(parser_error, context="DomclickParser._parse_internal")
            raise parser_error
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while fetching {url}: {e}")
            if e.response.status_code == 429:
                parser_error = RateLimitError(f"Rate limit exceeded (429)")
            elif e.response.status_code in (503, 502, 504):
                parser_error = NetworkError(f"Service unavailable ({e.response.status_code})")
            else:
                parser_error = NetworkError(f"HTTP error {e.response.status_code}: {e}")
            ParserErrorHandler.log_error(parser_error, context="DomclickParser._parse_internal")
            raise parser_error
        except httpx.RequestError as e:
            parser_error = ParserErrorHandler.convert_to_parser_exception(e)
            ParserErrorHandler.log_error(parser_error, context="DomclickParser._parse_internal")
            raise parser_error
        except Exception as e:
            parser_error = ParserErrorHandler.convert_to_parser_exception(e)
            ParserErrorHandler.log_error(parser_error, context="DomclickParser._parse_internal")
            raise parser_error
            
    async def validate_params(self, params: Dict[str, Any]) -> bool:
        """Validate Domclick-specific parameters."""
        if not params:
            return True
            
        # Add validation logic for Domclick-specific params here
        return True
        
    async def preprocess_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess parameters for Domclick parser."""
        processed = await super().preprocess_params(params)
        # Add Domclick-specific parameter preprocessing here
        return processed
        
    async def postprocess_results(self, results: List[PropertyCreate]) -> List[PropertyCreate]:
        """Postprocess results from Domclick parser."""
        processed = await super().postprocess_results(results)
        # Add Domclick-specific result postprocessing here
        return processed
        
    def _convert_location_to_slug(self, location: str) -> str:
        """Convert location name to URL slug."""
        location_mapping = {
            "москва": "moskva",
            "moscow": "moskva",
            "санкт-петербург": "sankt-peterburg",
            "spb": "sankt-peterburg",
            "saint petersburg": "sankt-peterburg",
            "казань": "kazan",
            "kazan": "kazan",
            "новосибирск": "novosibirsk",
            "ekaterinburg": "ekaterinburg",
            "екатеринбург": "ekaterinburg",
        }
        
        location_lower = location.lower()
        return location_mapping.get(location_lower, location_lower)
        
    def _build_query_params(self, params: Dict[str, Any]) -> Dict[str, str]:
        """Build query parameters for Domclick search."""
        query_params = {}
        
        # Add price filters
        if params and "min_price" in params and params["min_price"]:
            query_params["priceMin"] = str(int(params["min_price"]))
            
        if params and "max_price" in params and params["max_price"]:
            query_params["priceMax"] = str(int(params["max_price"]))
            
        # Add room count filter
        if params and "rooms" in params and params["rooms"]:
            rooms = params["rooms"]
            if isinstance(rooms, int):
                query_params["room"] = str(rooms)
                
        # Add area filters
        if params and "min_area" in params and params["min_area"]:
            query_params["areaMin"] = str(int(params["min_area"]))
            
        if params and "max_area" in params and params["max_area"]:
            query_params["areaMax"] = str(int(params["max_area"]))
                        
        return query_params
        
    async def _fetch_via_api(self, guid: str, deal: str, params: Dict[str, Any]) -> List[PropertyCreate]:
        """Выдача через BFF-search API: параллельная пагинация offset/limit."""
        from app.utils.fetch import fetch_many

        limit = DOMCLICK_PAGE_LIMIT
        pages = int((params or {}).get("pages") or DOMCLICK_PAGES)
        common = (
            f"address={guid}&limit={limit}&sort=qi&sort_dir=desc"
            f"&deal_type={deal}&category=living&offer_type=flat"
        )
        urls = [f"{DOMCLICK_API}?offset={o}&{common}" for o in range(0, limit * pages, limit)]

        bodies = await fetch_many(
            urls,
            concurrency=DOMCLICK_CONCURRENCY,
            headers={"accept": "application/json"},
        )

        items: list = []
        for body in bodies:
            if not body:
                continue
            try:
                data = json.loads(body)
                items.extend((data.get("result") or {}).get("items") or [])
            except Exception as e:
                logger.warning(f"Domclick API: bad json: {e}")

        logger.info(f"Domclick API: {len(items)} items from {len(urls)} pages")
        return self._parse_api_items(items)

    def _parse_api_items(self, items: list) -> List[PropertyCreate]:
        """Преобразовать items из API Домклика в PropertyCreate."""
        results: List[PropertyCreate] = []
        for it in items:
            try:
                info = it.get("objectInfo") or {}
                loc = it.get("location") or {}
                house = it.get("house") or {}
                addr = (it.get("address") or {}).get("displayName")

                bits = []
                if info.get("rooms"):
                    bits.append(f"{info['rooms']}-комн.")
                if info.get("area"):
                    bits.append(f"{info['area']} м²")
                cx = (it.get("flatComplex") or {}).get("name")
                if cx:
                    bits.append(str(cx))
                title = " ".join(bits) or (addr or "Объявление Домклик")

                photos = []
                for ph in (it.get("photos") or [])[:5]:
                    u = ph.get("url") if isinstance(ph, dict) else ph
                    if u and u.startswith("/"):
                        u = "https://domclick.ru" + u
                    if u:
                        photos.append(u)

                results.append(PropertyCreate(
                    source="domclick",
                    external_id=str(it.get("id") or ""),
                    title=title,
                    price=float(it.get("price") or 0),
                    link=it.get("path"),
                    rooms=info.get("rooms"),
                    area=info.get("area"),
                    floor=info.get("floor"),
                    total_floors=house.get("floors"),
                    address=addr,
                    latitude=loc.get("lat"),
                    longitude=loc.get("lon"),
                    price_per_sqm=float(it["squarePrice"]) if it.get("squarePrice") else None,
                    description=it.get("description"),
                    photos=photos,
                ))
            except Exception as e:
                logger.error(f"Domclick API item error: {e}")
        return results

    def _parse_html(self, html: str) -> list[PropertyCreate]:
        """Домклик — SSR: офферы лежат в window.__SSR_STATE__ (JSON в <script>).

        Парсим данные, а не DOM-карточки (в разметке карточек нет). В JSON
        встречается JS-литерал undefined — приводим его к null.
        """
        import json as _json

        blob = html.split("window.__SSR_STATE__=", 1)
        if len(blob) != 2:
            logger.warning("Domclick: window.__SSR_STATE__ не найден")
            return []
        body = re.sub(r"(?<=[:,\[])undefined", "null", blob[1].split("</script>", 1)[0]).strip().rstrip(";")
        try:
            data, _ = _json.JSONDecoder().raw_decode(body)
        except Exception as e:
            logger.error(f"Domclick: __SSR_STATE__ parse failed: {e}")
            return []

        offers: list = []

        def walk(node):
            if isinstance(node, dict):
                if "offerId" in node and ("price" in node or "path" in node):
                    offers.append(node)
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)

        walk(data)

        properties: list[PropertyCreate] = []
        for o in offers:
            try:
                info = o.get("generalInfo") or {}
                loc = o.get("location") or {}
                addr = (o.get("address") or {}).get("displayName")

                bits = []
                if info.get("rooms"):
                    bits.append(f"{info['rooms']}-комн.")
                if info.get("area"):
                    bits.append(f"{info['area']} м²")
                complex_name = (o.get("complex") or {}).get("name")
                if complex_name:
                    bits.append(str(complex_name))
                title = " ".join(bits) or (addr or "Объявление Домклик")

                photos = []
                for ph in (o.get("photos") or [])[:5]:
                    if isinstance(ph, dict):
                        u = ph.get("url") or ph.get("path")
                        if u:
                            photos.append(u)
                    elif isinstance(ph, str):
                        photos.append(ph)

                props = {
                    "source": "domclick",
                    "external_id": str(o.get("offerId") or o.get("id") or ""),
                    "title": title,
                    "price": float(o.get("price") or 0),
                    "link": o.get("path"),
                    "rooms": info.get("rooms"),
                    "area": info.get("area"),
                    "floor": info.get("minFloor"),
                    "total_floors": info.get("floors"),
                    "address": addr,
                    "latitude": loc.get("lat"),
                    "longitude": loc.get("lon"),
                    "photos": photos,
                }
                properties.append(PropertyCreate(**props))
            except Exception as e:
                logger.error(f"Error parsing domclick offer: {e}")

        logger.info(f"Domclick: parsed {len(properties)} offers from SSR state")
        return properties
        
    def _extract_rooms_from_title(self, title: str) -> int:
        # Look for patterns like "2-к", "2 комнатная", etc.
        room_patterns = [r'(\d+)[\-\s]*к', r'(\d+)\s*комнат', r'(\d+)\s*комн']
        
        for pattern in room_patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        return None
        
    def _extract_area_from_title(self, title: str) -> float:
        # Look for patterns like "45 м²", "50м2", etc.
        area_patterns = [r'(\d+(?:[\.,]\d+)?)\s*м\s*[²2]', r'(\d+(?:[\.,]\d+)?)\s*кв\.?\s*м']
        
        for pattern in area_patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1).replace(',', '.'))
                except ValueError:
                    continue
        return None
        
    def _extract_location(self, item, title: str = "") -> Optional[Dict[str, str]]:
        """Extract location information from item."""
        location = {}
        
        # Try to extract district/neighborhood from title
        if title:
            # Look for common Moscow districts in the title
            moscow_districts = [
                "центр", "цао", "север", "сао", "северо-восток", "свао", "восток", "вао",
                "юго-восток", "ювао", "юг", "юао", "юго-запад", "юзао", "запад", "зао",
                "северо-запад", "сзао", "зеленоград", "таганский", "басманный", "арбат",
                "якиманка", "хамовники", "кунцево", "перово", "люблино"
            ]
            
            for district in moscow_districts:
                if district in title.lower():
                    location["district"] = district
                    break
                    
        # Try to extract address if available
        address_elem = item.select_one("[data-testid='item-address']")
        if address_elem:
            location["address"] = address_elem.get_text(strip=True)
            
        return location if location else None
        
    def _extract_description(self, item) -> Optional[str]:
        """Extract description from item."""
        # Look for description element
        desc_elem = item.select_one("[data-testid='item-description']")
        if desc_elem:
            return desc_elem.get_text(strip=True)
            
        return None
        
    def _extract_external_id(self, link: str) -> str:
        """Extract external ID from link."""
        # Extract ID from URL like https://domclick.ru/listing/123456789
        match = re.search(r'/listing/(\d+)/?$', link)
        return match.group(1) if match else ""
