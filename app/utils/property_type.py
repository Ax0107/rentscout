"""Канонический тип объекта — из данных площадки, а не из среза запроса.

``property_type`` в выдаче должен описывать САМ объект, а слово из запроса
(``--types=house``) — быть только фолбэком: площадка могла проигнорировать
фильтр и вернуть квартиры, и раньше такие объекты размечались «домом».

Тип достаём из данных площадки:
  * Домклик (BFF-search API) — ``offerType`` ("flat"/"house"/"room"/…), в
    пути карточки — ``rent__<offerType>__<id>``;
  * Домклик 24 (посуточно) — ``category`` ("flat"/…);
  * Циан — маршрут карточки ``/rent/<slug>/`` ("flat"/"room"/"suburban"/…);
  * Яндекс.Недвижимость — alt картинки/заголовок и маршрут ``/snyat/<slug>/``.

Словарь значений — короткие английские токены, те же, что AIOPA присылает
в ``type=`` (``apartment``/``house``/``room``/…).
"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse

APARTMENT = "apartment"
ROOM = "room"
HOUSE = "house"
TOWNHOUSE = "townhouse"
COMMERCIAL = "commercial"
GARAGE = "garage"
LAND = "land"

# Синонимы (ru/en) и машинные токены площадок → канонический тип.
_SYNONYMS = {
    # квартира / апартаменты
    "apartment": APARTMENT, "flat": APARTMENT, "квартира": APARTMENT,
    "апартамент": APARTMENT, "апартаменты": APARTMENT,
    "studio": APARTMENT, "студия": APARTMENT, "студио": APARTMENT,
    "kvartira": APARTMENT,
    # комната
    "room": ROOM, "rooms": ROOM, "комната": ROOM, "komnata": ROOM,
    # дом / загородная
    "house": HOUSE, "дом": HOUSE, "коттедж": HOUSE, "дача": HOUSE,
    "suburban": HOUSE, "dom": HOUSE, "doma": HOUSE,
    # таунхаус
    "townhouse": TOWNHOUSE, "таунхаус": TOWNHOUSE,
    # коммерция
    "commercial": COMMERCIAL, "коммерческая": COMMERCIAL,
    "office": COMMERCIAL, "офис": COMMERCIAL,
    "warehouse": COMMERCIAL, "склад": COMMERCIAL,
    "kommercheskaya": COMMERCIAL,
    # гараж / машиноместо
    "garage": GARAGE, "гараж": GARAGE, "parking": GARAGE,
    "машиноместо": GARAGE, "garazh": GARAGE,
    # участок
    "land": LAND, "lot": LAND, "участок": LAND,
    "uchastok": LAND, "zemlya": LAND,
}

# Подстроки для разбора свободного текста (заголовок/alt карточки).
# Порядок = приоритет: «1-комнатная квартира» — это квартира, не комната.
_TEXT_PATTERNS = (
    (APARTMENT, re.compile(r"квартир|апартамент|студи|apartment|\bflat\b")),
    (ROOM, re.compile(r"комнат[ауеы]\b|\broom\b")),
    (TOWNHOUSE, re.compile(r"таунхаус|townhouse")),
    (HOUSE, re.compile(r"\bдом[ауе]?\b|коттедж|\bдач|suburban|\bhouse\b")),
    (COMMERCIAL, re.compile(r"коммерческ|офис|склад|магазин|office|warehouse|commercial")),
    (GARAGE, re.compile(r"гараж|машиноместо|parking|garage")),
    (LAND, re.compile(r"участок|земельн|\bземл|\bland\b")),
)


def normalize(value: Optional[str]) -> Optional[str]:
    """Слово типа (ru/en/токен площадки) → канонический тип или None."""
    if value is None:
        return None
    token = str(value).strip().lower()
    if not token:
        return None
    if token in _SYNONYMS:
        return _SYNONYMS[token]
    return detect_from_text(token)


def detect_from_text(text: Optional[str]) -> Optional[str]:
    """Свободный текст карточки → канонический тип (или None)."""
    if not text:
        return None
    t = str(text).lower()
    for canonical, pattern in _TEXT_PATTERNS:
        if pattern.search(t):
            return canonical
    return None


def detect_from_url(url: Optional[str]) -> Optional[str]:
    """Маршрут карточки → канонический тип (или None).

    Берём сегменты пути и ищем среди них известный токен: ``/rent/flat/123``
    у Циана, ``/snyat/kvartira/metro-x/`` у Яндекса, ``/card/rent__house__1``
    у Домклика.
    """
    if not url:
        return None
    path = urlparse(str(url)).path.lower()
    for token in re.split(r"[/_\-]+", path):
        canonical = _SYNONYMS.get(token)
        if canonical:
            return canonical
    return None
