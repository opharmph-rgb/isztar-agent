from fastapi import FastAPI, HTTPException
import requests
import time
import os

app = FastAPI()

BASE = "https://ext-isztar4.mf.gov.pl/tariff/rest/goods-nomenclature"

# cache w pamięci
CODES_CACHE = []
CODES_CACHE_META = {
    "built": False,
    "count": 0,
    "last_build_seconds": None,
    "last_page": None,
    "date": None,
    "language": None,
    "last_error": None,
}


def _walk_subgroup_tree(node, out):
    """Rekurencyjnie zbiera wszystkie liście z code/description w strukturze subgroup."""
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


def _parse_codes_tree(json_data):
    out = []
    _walk_subgroup_tree(json_data, out)
    return out


def build_codes_cache(date="2025-11-17", language="PL", pages=3, time_budget_seconds=25):
    """
    Bezpieczna budowa indeksu:
    - domyślnie tylko kilka stron (pages=3)
    - limit czasu
    """
    global CODES_CACHE, CODES_CACHE_META

    start = time.time()
    items = []
    last_ok_page = 0

    # reset meta (ale nie wywalaj cache jeśli budowa padnie)
    CODES_CACHE_META.update({
        "built": False,
        "count": 0,
        "last_build_seconds": None,
        "last_page": None,
        "date": date,
        "language": language,
        "last_error": None,
    })

    try:
        for page in range(1, max(1, int(pages)) + 1):
            if (time.time() - start) > time_budget_seconds:
                break

            r = requests.get(
                f"{BASE}/codes",
                params={"date": date, "language": language, "page": page},
                timeout=30,
            )

            # jak nie ma strony lub walidacja padła, kończymy
            if r.status_code in (404, 422):
                break

            r.raise_for_status()
            data = r.json()

            page_items = _parse_codes_tree(data)
            if not page_items:
                # jeśli na tej stronie nic nie było, kończymy
                break

            items.extend(page_items)
            last_ok_page = page

        # deduplikacja po code
        uniq = {}
        for it in items:
            c = it.get("code")
            if c:
                uniq[c] = it

        CODES_CACHE = list(uniq.values())
        CODES_CACHE_META["built"] = True
        CODES_CACHE_META["count"] = len(CODES_CACHE)
        CODES_CACHE_META["last_build_seconds"] = round(time.time() - start, 2)
        CODES_CACHE_META["last_page"] = last_ok_page if last_ok_page else None

    except Exception as e:
        # zostaw czytelną informację o błędzie
        CODES_CACHE_META["last_error"] = repr(e)
        raise


@app.get("/")
def home():
    return {"status": "ok"}


@app.get("/index_status")
def index_status():
    return CODES_CACHE_META


@app.get("/rebuild_index")
def rebuild_index(date: str = "2025-11-17", language: str = "PL", pages: int = 3):
    """
    Najpierw zbuduj mały indeks (pages=3).
    Potem możesz zwiększać pages (np. 10, 30, 100).
    """
    try:
        build_codes_cache(date=date, language=language, pages=pages)
        return {"ok": True, "meta": CODES_CACHE_META}
    except Exception as e:
        # zwróć błąd w JSON, zamiast „Internal Server Error” bez info
        raise HTTPException(status_code=500, detail={"error": repr(e), "meta": CODES_CACHE_META})


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
    # jeśli brak indeksu, buduj malutki (pages=3), żeby nie zabijać serwera
    if (
        not CODES_CACHE_META["built"]
        or CODES_CACHE_META.get("date") != date
        or CODES_CACHE_META.get("language") != language
    ):
        try:
            build_codes_cache(date=date, language=language, pages=3)
        except Exception:
            # jak budowa padła, i tak pokaż meta z last_error
            pass

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
