from fastapi import FastAPI
import requests
import time
from urllib.parse import urlparse, parse_qs

app = FastAPI()

BASE = "https://ext-isztar4.mf.gov.pl/tariff/rest/goods-nomenclature"

# Cache w pamięci (na Render Free może się resetować po uśpieniu)
CODES_CACHE = []
CODES_CACHE_META = {
    "built": False,
    "count": 0,
    "last_build_seconds": None,
    "last_page": None,
    "date": None,
    "language": None,
}


def _extract_last_page(json_data):
    """Wyciąga numer ostatniej strony z links.last (JSON:API)."""
    if not isinstance(json_data, dict):
        return None
    links = json_data.get("links") or {}
    last_url = links.get("last")
    if not last_url:
        return None
    try:
        qs = parse_qs(urlparse(last_url).query)
        page_vals = qs.get("page")
        if page_vals:
            return int(page_vals[0])
    except Exception:
        return None
    return None


def _parse_codes_response(json_data):
    """
    ISZTAR /codes jest w JSON:API.
    Najczęściej:
      - json_data["data"] = lista rekordów
      - rekord ma "attributes" z polami: code, description
    Ten parser jest "odporny" na drobne różnice nazw pól.
    """
    if not isinstance(json_data, dict):
        return []

    rows = json_data.get("data") or []
    if not isinstance(rows, list):
        return []

    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        attrs = row.get("attributes") or {}
        if not isinstance(attrs, dict):
            attrs = {}

        # możliwe nazwy kodu
        code = (
            attrs.get("code")
            or attrs.get("goodsNomenclatureItemId")
            or attrs.get("nomenclatureCode")
            or row.get("id")
        )

        # możliwe nazwy opisu
        desc = (
            attrs.get("description")
            or attrs.get("formattedDescription")
            or attrs.get("descriptionFormatted")
            or attrs.get("description_formatted")
        )

        if code and desc:
            out.append({"code": str(code).strip(), "description": str(desc).strip()})

    return out


def build_codes_cache(date="2025-11-17", language="PL", max_pages=5000, time_budget_seconds=25):
    """
    Pobiera kolejne strony /codes i buduje cache.
    Zabezpieczenia:
      - max_pages (twardy limit)
      - time_budget_seconds (żeby nie wisiało)
    """
    global CODES_CACHE, CODES_CACHE_META

    start = time.time()
    page = 1
    last_page = None
    items = []

    while page <= max_pages:
        if (time.time() - start) > time_budget_seconds:
            break

        r = requests.get(
            f"{BASE}/codes",
            params={"date": date, "language": language, "page": page},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()

        if last_page is None:
            lp = _extract_last_page(data)
            if lp:
                last_page = lp

        page_items = _parse_codes_response(data)

        # jeśli nagle nic nie przyszło, kończymy (albo struktura, albo koniec)
        if not page_items:
            # ale nie kończ na stronie 1 bez diagnostyki — zostaw meta i przerwij
            break

        items.extend(page_items)

        if last_page and page >= last_page:
            break

        page += 1

    # deduplikacja po code
    uniq = {}
    for it in items:
        if it["code"]:
            uniq[it["code"]] = it

    CODES_CACHE = list(uniq.values())

    CODES_CACHE_META["built"] = True
    CODES_CACHE_META["count"] = len(CODES_CACHE)
    CODES_CACHE_META["last_build_seconds"] = round(time.time() - start, 2)
    CODES_CACHE_META["last_page"] = last_page if last_page else page
    CODES_CACHE_META["date"] = date
    CODES_CACHE_META["language"] = language


@app.get("/")
def home():
    return {"status": "ok"}


@app.get("/index_status")
def index_status():
    return CODES_CACHE_META


@app.get("/rebuild_index")
def rebuild_index(date: str = "2025-11-17", language: str = "PL"):
    build_codes_cache(date=date, language=language)
    return {"ok": True, "meta": CODES_CACHE_META}


@app.get("/debug_codes_page")
def debug_codes_page(date: str = "2025-11-17", language: str = "PL", page: int = 1):
    """Podgląd surowej odpowiedzi ISZTAR /codes dla jednej strony."""
    r = requests.get(
        f"{BASE}/codes",
        params={"date": date, "language": language, "page": page},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


@app.get("/measures")
def measures(code: str, date: str = "2025-11-17", language: str = "PL"):
    r = requests.get(
        f"{BASE}/measures",
        params={"nomenclatureCode": code, "date": date, "language": language},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


@app.get("/search_codes")
def search_codes(
    q: str,
    date: str = "2025-11-17",
    language: str = "PL",
    limit: int = 30,
):
    """
    Szuka po opisie w lokalnym cache.
    Jeśli cache nie jest zbudowany — buduje go przy pierwszym wywołaniu.
    """
    if not CODES_CACHE_META["built"] or CODES_CACHE_META.get("date") != date or CODES_CACHE_META.get("language") != language:
        build_codes_cache(date=date, language=language)

    q_low = q.lower().strip()
    hits = []
    for it in CODES_CACHE:
        if q_low in it["description"].lower():
            hits.append(it)
            if len(hits) >= max(1, limit):
                break

    return {
        "query": q,
        "count": len(hits),
        "items": hits,
        "index_meta": CODES_CACHE_META,
    }
