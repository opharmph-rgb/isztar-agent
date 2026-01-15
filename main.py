from fastapi import FastAPI
import requests
import time
from urllib.parse import urlparse, parse_qs

app = FastAPI()

BASE = "https://ext-isztar4.mf.gov.pl/tariff/rest/goods-nomenclature"

# --- Prosty cache w pamięci (na Free Render może się resetować po uśpieniu) ---
CODES_CACHE = []
CODES_CACHE_META = {"built": False, "count": 0, "last_build_seconds": None, "last_page": None}


def _extract_last_page(json_data):
    """
    ISZTAR4 zwraca JSON:API z 'links'. W linku 'last' często jest page=...
    Jeśli nie znajdziemy, zwracamy None.
    """
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


def build_codes_cache(date="2025-11-17", language="PL", max_pages=5000, time_budget_seconds=25):
    """
    Pobiera wszystkie strony /codes i buduje cache.
    Zabezpieczenia:
    - limit max_pages
    - limit czasu time_budget_seconds (żeby nie wisiało)
    """
    global CODES_CACHE, CODES_CACHE_META

    start = time.time()
    page = 1
    last_page = None
    items = []

    while page <= max_pages:
        # przerwij jeśli przekroczysz budżet czasu
        if (time.time() - start) > time_budget_seconds:
            break

        r = requests.get(f"{BASE}/codes", params={"date": date, "language": language, "page": page}, timeout=30)
        r.raise_for_status()
        data = r.json()

        # ustal last_page, jeśli API podaje link "last"
        if last_page is None:
            lp = _extract_last_page(data)
            if lp:
                last_page = lp

        rows = data if isinstance(data, list) else data.get("results", [])
        if not rows:
            # koniec listy
            break

        for row in rows:
            code = str(row.get("code", "")).strip()
            desc = str(row.get("description", "")).strip()
            if code and desc:
                items.append({"code": code, "description": desc})

        # jeśli znamy last_page i ją osiągnęliśmy
        if last_page and page >= last_page:
            break

        page += 1

    # deduplikacja
    uniq = {}
    for it in items:
        uniq[it["code"]] = it

    CODES_CACHE = list(uniq.values())
    CODES_CACHE_META["built"] = True
    CODES_CACHE_META["count"] = len(CODES_CACHE)
    CODES_CACHE_META["last_build_seconds"] = round(time.time() - start, 2)
    CODES_CACHE_META["last_page"] = last_page if last_page else page


@app.get("/")
def home():
    return {"status": "ok"}


@app.get("/index_status")
def index_status():
    return CODES_CACHE_META


@app.get("/rebuild_index")
def rebuild_index(date: str = "2025-11-17", language: str = "PL"):
    # ręczne przebudowanie cache
    build_codes_cache(date=date, language=language)
    return {"ok": True, "meta": CODES_CACHE_META}


@app.get("/measures")
def measures(code: str, date: str = "2025-11-17", language: str = "PL"):
    url = f"{BASE}/measures"
    r = requests.get(
        url,
        params={"nomenclatureCode": code, "date": date, "language": language},
        timeout=30
    )
    r.raise_for_status()
    return r.json()


@app.get("/search_codes")
def search_codes(q: str, date: str = "2025-11-17", language: str = "PL", limit: int = 30):
    """
    Szuka po opisie w lokalnym cache.
    Jeśli cache nie jest zbudowany — buduje go przy pierwszym wywołaniu.
    """
    if not CODES_CACHE_META["built"]:
        build_codes_cache(date=date, language=language)

    q_low = q.lower().strip()
    hits = []
    for it in CODES_CACHE:
        if q_low in it["description"].lower():
            hits.append(it)
            if len(hits) >= max(1, limit):
                break

    return {"query": q, "count": len(hits), "items": hits, "index_meta": CODES_CACHE_META}
