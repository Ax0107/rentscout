import asyncio
import httpx
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
from app.utils.property_type import detect_from_text, detect_from_url

logger = logging.getLogger(__name__)

# Сколько страниц выдачи тянуть и с какой параллельностью (cat.php ~28 карточек/стр.).
CIAN_PAGES = int(os.getenv("CIAN_PAGES", "100"))
CIAN_CONCURRENCY = int(os.getenv("CIAN_CONCURRENCY", "5"))


class CianParser(BaseParser):
    BASE_URL = "https://www.cian.ru"

    # Город → поддомен для посуточной выдачи (/snyat-kvartiru-posutochno/).
    # На www.cian.ru регион в URL не учитывается, город задаётся поддоменом.
    SHORT_SUBDOMAINS = {
        "москва": "www", "moscow": "www",
        "санкт-петербург": "spb", "спб": "spb", "saint petersburg": "spb",
    }
    
    def __init__(self):
        super().__init__()
        self.name = "CianParser"
        
    @cached_parser(expire=600, source="cian")  # Кеш на 10 минут
    @retry(max_attempts=3, initial_delay=1.0, max_delay=10.0)
    @metrics_collector_decorator
    async def parse(self, location: str, params: Dict[str, Any] = None) -> List[PropertyCreate]:
        # Проверяем circuit breaker перед попыткой
        circuit_breaker = get_circuit_breaker("cian")

        try:
            return await circuit_breaker.call_async(self._parse_internal, location, params)
        except CircuitBreakerOpen as e:
            logger.warning(f"Cian parser circuit is open: {e}")
            raise NetworkError(f"Cian service temporarily unavailable: {e}")

    async def _parse_internal(self, location: str, params: Dict[str, Any]) -> List[PropertyCreate]:
        """Внутренний метод парсинга."""
        # Preprocess params
        processed_params = await self.preprocess_params(params)

        # Посуточная аренда — отдельный раздел Циана с другим фронтендом и
        # разметкой (карточки на data-testid, данные готовым JSON "offers");
        # обычный cat.php?deal_type=rent отдаёт только долгосрочную выдачу.
        is_short = (processed_params or {}).get("operation") == "rent_short"
        if is_short:
            url = self._short_url(location)
            query_params: Dict[str, str] = {}
            parse_html = self._parse_short_html
            pages = 1
        else:
            url = f"{self.BASE_URL}/cat.php"
            query_params = self._build_query_params(location, processed_params)
            parse_html = self._parse_html
            pages = int((processed_params or {}).get("pages") or CIAN_PAGES)

        # Apply rate limiting
        await rate_limiter.acquire("cian")

        try:
            # 1) Основной путь — curl_cffi с подменой TLS-отпечатка, без браузера.
            #    Страницы тянем параллельно и склеиваем с дедупом по ссылке.
            from urllib.parse import urlencode

            from app.utils.fetch import fetch_many

            urls = []
            for p in range(1, pages + 1):
                qp = dict(query_params)
                if p > 1:
                    qp["p"] = p
                urls.append(f"{url}?{urlencode(qp)}" if qp else url)

            htmls = await fetch_many(urls, concurrency=CIAN_CONCURRENCY)

            items: List[PropertyCreate] = []
            seen: set = set()
            for html in htmls:
                if not html:
                    continue
                for it in parse_html(html):
                    link = getattr(it, "link", None)
                    if link and link in seen:
                        continue
                    if link:
                        seen.add(link)
                    items.append(it)

            if items:
                return await self.postprocess_results(items)

            # 2) Последний фолбэк — обычный HTTP-пул.
            from app.utils.http_pool import OptimizedHTTPClient

            async with OptimizedHTTPClient(name="cian") as client:
                response = await client.get(url, params=query_params or None)
                results = parse_html(response.text)
                return await self.postprocess_results(results)

        except asyncio.TimeoutError as e:
            parser_error = ParserTimeoutError(f"Timeout while fetching {url}: {e}")
            ParserErrorHandler.log_error(parser_error, context="CianParser._parse_internal")
            raise parser_error
        except httpx.TimeoutException as e:
            parser_error = ParserTimeoutError(f"HTTP timeout: {e}")
            ParserErrorHandler.log_error(parser_error, context="CianParser._parse_internal")
            raise parser_error
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while fetching {url}: {e}")
            if e.response.status_code == 429:
                parser_error = RateLimitError(f"Rate limit exceeded (429)")
            elif e.response.status_code in (503, 502, 504):
                parser_error = NetworkError(f"Service unavailable ({e.response.status_code})")
            else:
                parser_error = NetworkError(f"HTTP error {e.response.status_code}: {e}")
            ParserErrorHandler.log_error(parser_error, context="CianParser._parse_internal")
            raise parser_error
        except httpx.RequestError as e:
            parser_error = ParserErrorHandler.convert_to_parser_exception(e)
            ParserErrorHandler.log_error(parser_error, context="CianParser._parse_internal")
            raise parser_error
        except Exception as e:
            parser_error = ParserErrorHandler.convert_to_parser_exception(e)
            ParserErrorHandler.log_error(parser_error, context="CianParser._parse_internal")
            raise parser_error
            
    async def validate_params(self, params: Dict[str, Any]) -> bool:
        """Validate Cian-specific parameters."""
        if not params:
            return True
            
        # Add validation logic for Cian-specific params here
        return True
        
    async def preprocess_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Preprocess parameters for Cian parser."""
        processed = await super().preprocess_params(params)
        # Add Cian-specific parameter preprocessing here
        return processed
        
    async def postprocess_results(self, results: List[PropertyCreate]) -> List[PropertyCreate]:
        """Postprocess results from Cian parser."""
        processed = await super().postprocess_results(results)
        # Add Cian-specific result postprocessing here
        return processed
        
    def _build_query_params(self, location: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Build query parameters for Cian search."""
        query_params = {
            "engine_version": "2",
            "offer_type": "rent",
            "type": "4",  # Квартиры
            "currency": "2",  # RUB
            "deal_type": "rent",
            "region": "1",  # Москва (по умолчанию)
        }
        
        # Map location to region code if needed
        # In a real implementation, this would map city names to CIAN region IDs
        if location.lower() in ["москва", "moscow"]:
            query_params["region"] = "1"
        elif location.lower() in ["санкт-петербург", "spb", "saint petersburg"]:
            query_params["region"] = "2"
            
        # Тип объекта. AIOPA шлёт АНГЛИЙСКИЕ имена (apartment/house/…), а здесь
        # исторически ждали русские — из-за этого фильтр не применялся вовсе,
        # и воркер штамповал объекту имя среза. Понимаем оба набора; чего не
        # умеем выразить — не выдаём за отфильтрованное (только пишем в лог).
        if params and "type" in params:
            codes = {
                "квартира": "4", "apartment": "4", "flat": "4",
                "комната": "5", "room": "5",
                "дом": "3", "house": "3", "townhouse": "3",
            }
            code = codes.get(str(params["type"]).lower())
            if code:
                query_params["type"] = code
            else:
                logger.warning(
                    "cian: тип '%s' не выражается в запросе — срез идёт без фильтра типа",
                    params["type"],
                )
                
        # Add price filters
        if params and "min_price" in params and params["min_price"]:
            query_params["price_min"] = str(int(params["min_price"]))
            
        if params and "max_price" in params and params["max_price"]:
            query_params["price_max"] = str(int(params["max_price"]))
            
        # Add room count filter
        if params and "rooms" in params and params["rooms"]:
            rooms = params["rooms"]
            if isinstance(rooms, int):
                query_params["room"] = str(rooms)
            elif isinstance(rooms, list):
                # Handle multiple room options
                room_params = []
                for room in rooms:
                    if isinstance(room, int) and 1 <= room <= 5:
                        room_params.append(f"room{room}")
                if room_params:
                    query_params.update({rp: "1" for rp in room_params})
                        
        return query_params

    def _short_url(self, location: str) -> str:
        """URL посуточной выдачи Циана для города (Москва → www.cian.ru)."""
        sub = self.SHORT_SUBDOMAINS.get((location or "").strip().lower(), "www")
        return f"https://{sub}.cian.ru/snyat-kvartiru-posutochno/"

    def _parse_short_html(self, html: str) -> list[PropertyCreate]:
        """Посуточная выдача Циана (раздел /snyat-kvartiru-posutochno/).

        У неё другой фронтенд (dailyrent-search-frontend): данные лежат готовым
        JSON в "offers":[...] — цена за сутки, площадь, фото, гео. Если JSON не
        нашёлся — разбираем карточки по data-testid.
        """
        offers = self._extract_short_json(html)
        if offers:
            return self._short_from_json(offers)
        return self._short_from_dom(html)

    @staticmethod
    def _extract_short_json(html: str) -> list:
        """Вытащить массив offers из встроенного JSON страницы посуточной выдачи."""
        import json

        marker = '"offers":['
        i = html.find(marker)
        if i < 0:
            return []
        try:
            arr, _ = json.JSONDecoder().raw_decode(html[i + len(marker) - 1:].strip())
        except Exception:
            return []
        return [o for o in arr if isinstance(o, dict) and o.get("cianId")]

    def _short_from_json(self, offers: list) -> list[PropertyCreate]:
        results: list[PropertyCreate] = []
        for o in offers:
            try:
                feats = o.get("features") or {}
                geo = o.get("geo") or {}
                price = (o.get("price") or {}).get("value") or 0
                obj_type = feats.get("objectType") or ""
                rooms = self._extract_rooms_from_title(obj_type)
                if rooms is None and "студия" in obj_type.lower():
                    rooms = 0
                floor = total_floors = None
                m = re.match(r"\s*(\d+)\s*/\s*(\d+)", feats.get("floorInfo") or "")
                if m:
                    floor, total_floors = int(m.group(1)), int(m.group(2))
                photos = [
                    p.get("url") for p in (o.get("media") or [])
                    if isinstance(p, dict) and p.get("url")
                ]
                coords = geo.get("coordinates") or {}
                addr = geo.get("address") or []
                address = ", ".join(
                    x.get("value", "") for x in addr
                    if isinstance(x, dict) and x.get("value")
                ) or None
                cian_id = o.get("cianId")
                link = f"{self.BASE_URL}/rent/flat/{cian_id}/" if cian_id else None
                results.append(PropertyCreate(
                    source="cian",
                    external_id=str(cian_id or ""),
                    title=obj_type or "Посуточная аренда Циан",
                    description=(o.get("description") or "")[:2000] or None,
                    link=link,
                    price=float(price or 0),
                    property_type=detect_from_text(obj_type) or detect_from_url(link),
                    rooms=rooms,
                    area=self._extract_area_from_title(feats.get("totalArea") or ""),
                    floor=floor,
                    total_floors=total_floors,
                    address=address,
                    latitude=coords.get("lat"),
                    longitude=coords.get("lng"),
                    photos=photos[:5],
                    location=geo or None,
                ))
            except Exception as e:
                logger.error(f"Error parsing cian short offer: {e}")
        return results

    def _short_from_dom(self, html: str) -> list[PropertyCreate]:
        """Фолбэк: разбор карточек посуточной выдачи по data-testid."""
        soup = BeautifulSoup(html, "lxml")
        results: list[PropertyCreate] = []
        for card in soup.select("[data-testid='OfferCard']"):
            try:
                link_el = card.select_one("a[href*='/rent/flat/']")
                link = link_el.get("href", "") if link_el else ""
                if link and not link.startswith("http"):
                    link = self.BASE_URL + link
                price_el = card.select_one("[data-testid='price']")
                price_text = price_el.get_text(" ", strip=True) if price_el else ""
                m_price = re.search(r"\d[\d\s\u00a0]*", price_text)
                price = float(re.sub(r"\D", "", m_price.group(0))) if m_price else 0
                img = card.select_one("img[alt]")
                title = (img.get("alt") if img else "") or "Посуточная аренда Циан"
                photos = [
                    i.get("src") for i in card.select("img")
                    if i.get("src") and "placeholder" not in i["src"]
                ]
                results.append(PropertyCreate(
                    source="cian",
                    external_id=self._extract_external_id(link),
                    title=title,
                    link=link or None,
                    price=price,
                    property_type=detect_from_text(title) or detect_from_url(link),
                    rooms=self._extract_rooms_from_title(title),
                    area=self._extract_area_from_title(title),
                    photos=photos[:5],
                ))
            except Exception as e:
                logger.error(f"Error parsing cian short card: {e}")
        return results

    def _parse_html(self, html: str) -> list[PropertyCreate]:
        soup = BeautifulSoup(html, "lxml")
        properties = []
        
        # Find property listings
        # Note: Actual selectors would need to be updated based on CIAN's current HTML structure
        listings = soup.select("[data-name='CardComponent']")
        
        for item in listings:
            try:
                # Extract basic information
                title_elem = item.select_one("[data-mark='OfferTitle']") or item.select_one("[data-mark='OfferSubtitle']") or item.select_one("[data-name='Title']")
                price_elem = item.select_one("[data-mark='MainPrice']") or item.select_one("[data-name='Price']")
                
                if title_elem is None and price_elem is None:
                    continue
                    
                # Extract title
                title = title_elem.get_text(strip=True) if title_elem else ""
                
                # Extract price (remove non-numeric characters except decimal point)
                price_text = price_elem.get_text(" ", strip=True) if price_elem else ""
                price_digits = re.sub(r"\D", "", price_text)
                price = float(price_digits) if price_digits else 0
                
                # Extract link
                link_elem = item.select_one("a")
                link = link_elem.get("href", "") if link_elem else ""
                if link and not link.startswith("http"):
                    link = self.BASE_URL + link
                    
                # Parse title for rooms and area information
                rooms = self._extract_rooms_from_title(title)
                area = self._extract_area_from_title(title)
                
                # Extract photos
                photos = []
                img_elements = item.select("img")
                for img in img_elements:
                    if img.get("src"):
                        photos.append(img["src"])
                        
                # Extract additional information
                location = self._extract_location(item, title)
                description = self._extract_description(item)
                
                props = {
                    "source": "cian",
                    "external_id": self._extract_external_id(link),
                    "title": title,
                    "price": price,
                    "link": link,
                    # Тип — из маршрута карточки (/rent/flat/, /rent/suburban/),
                    # с фолбэком на подпись: не из среза запроса.
                    "property_type": detect_from_url(link) or detect_from_text(title),
                    "rooms": rooms,
                    "area": area,
                    "photos": photos[:5],  # Limit to 5 photos
                    "location": location,
                    "description": description,
                }
                properties.append(PropertyCreate(**props))
            except Exception as e:
                logger.error(f"Error parsing item: {e}")
                
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
        address_elem = item.select_one("[data-name='AddressLine']") or item.select_one("[data-mark='Address']")
        if address_elem:
            location["address"] = address_elem.get_text(strip=True)
            
        return location if location else None
        
    def _extract_description(self, item) -> Optional[str]:
        """Extract description from item."""
        # Look for description element
        desc_elem = item.select_one("[data-name='Description']") or item.select_one("[data-mark='Description']")
        if desc_elem:
            return desc_elem.get_text(strip=True)
            
        return None
        
    def _extract_external_id(self, link: str) -> str:
        """Extract external ID from link."""
        # Extract ID from URL like https://www.cian.ru/rent/flat/123456789/
        match = re.search(r'/(\d+)/?$', link)
        return match.group(1) if match else ""
