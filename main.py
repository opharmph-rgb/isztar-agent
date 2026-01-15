from fastapi import FastAPI
import requests
import time

app = FastAPI()

BASE = "https://ext-isztar4.mf.gov.pl/tariff/rest/goods-nomenclature"

CODES_CACHE = []
CODES_CACHE_META = {
    "built": False,
    "count": 0,
    "last_build_seconds": None,
    "last_page": None,
    "date": None,
    "language": None,
}


def _walk_subgroup_tree(node, out):
    """
    Twoje /codes zwraca drzewo:
      {description, subgroup:[...]} i czasem {code, description}
    Przechodzimy rekurencyjnie i zbieramy wszystkie liście z code.
    """
    if isinstance(node, dict):
        code = node.get("code")
        desc = node.get("description")
        if code and desc:
            out.append({"code": str(code).strip(), "description": str(desc).strip()})

        subgroup = node.get("subgroup")
        if isinstance(subgroup, list):
            for child in subgroup:
                _walk_subgroup_tree(child, out)

    elif isinstance(node, list):
        for item in node:
            _walk_subgroup_tree(item, out)


def _parse_codes_response(json_data):
    """
    Obsługujemy 2 możliwe formaty:
    1) JSON:API (data/attributes) – na wszelki wypadek
    2) DRZEWO (description/subgroup/code) – to co Ty pokazałeś
    """
    out = []

    # (1) JSON:API (fallback)
    if isinstance(json_data, dict) and isinstance(json_data.get("data"), list):
        for row in json_data["data"]:
            if not isinstance(row, dict):
                continue
            attrs = row.get("attributes") or {}
            if not isinstance(attrs, dict):
                attrs = {}
            code = attrs.get("code") or attrs.get("goodsNomenclatureItemId") or row.get("id")
            desc = attrs.get("description") or attrs.get("formattedDescription")
            if code and desc:
                out.append({"code": str(code).strip(), "description": str(desc).strip()})
        return out

    # (2) Drzewo subgroup (TO U CIEBIE)
    _walk_subgroup_tree(json_data, out)
    return out


def build_codes_cache(date="2025-11-17", language="PL", max_pages=200, time_budget_seconds=40):
    """
    Pobiera kolejne strony /codes?page=1..N i buduje cache.
    Kończymy, gdy:
    - przekroczymy limit czasu
    - dostaniemy 404/422
    - albo parser nie znalazł NIC na danej stronie
    """
    global CODES_CACHE, CODES_CACHE_META

    start = time.time()
    items = []
    page = 1
    last_ok_page = 0

    while page <= max_pages:
        if (time.time() - start) > time_budget_seconds:
            break

        r = requests.get(
            f"{BASE}/codes",
            params={"date": date, "language": language, "page": page},
            timeout=30,
        )

        # jeśli API mówi "nie ma tej strony" – kończymy
        if r.status_code in (404, 422):
            break

        r.raise_for_status()
        data = r.json()

        page_items = _parse_codes_response(data)

        # jeśli nic nie znaleźliśmy na stronie -> kończymy
        if not page_items:
            break

        items.extend(page_items)
        last_ok_page = page
        page += 1

    # deduplikacja po code
    uniq = {}
    for it in items:
        if it.get("code"):
            uniq[it["code"]] = it

    CODES_CACHE = list(uniq.values())

    CODES_CACHE_META["built"] = True
    CODES_CACHE_META["count"] = len(CODES_CACHE)
    CODES_CACHE_META["last_build_seconds"] = round(time.time() - start, 2)
    CODES_CACHE_META["last_page"] = last_ok_page if last_ok_page else None
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
def search_codes(q: str, date: str = "2025-11-17", language: str = "PL", limit: int = 30):
    # auto-build, jeśli brak indeksu lub inna data/język
    if (
        not CODES_CACHE_META["built"]
        or CODES_CACHE_META.get("date") != date
        or CODES_CACHE_META.get("language") != language
    ):
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
