from fastapi import FastAPI, HTTPException
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
    "last_error": None,
}


def _normalize_text(s: str) -> str:
    # proste czyszczenie: myślniki/duże spacje
    s = (s or "").strip()
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def _walk_tree_with_context(node, out, context):
    """
    Rekurencyjnie idziemy po drzewie subgroup.
    context = lista opisów rodziców (nagłówków).
    Gdy trafimy na liść z code, to do jego opisu dopisujemy kontekst.
    """
    if isinstance(node, dict):
        desc = node.get("description")
        desc_norm = _normalize_text(desc) if isinstance(desc, str) else None

        # aktualizujemy kontekst, ale tylko jeśli jest sensowny opis
        new_context = context
        if desc_norm:
            new_context = context + [desc_norm]

        code = node.get("code")
        if code and desc_norm:
            # zbuduj opis z kontekstem (unikalne, bez powtórzeń)
            # np. "ZWIERZĘTA ŻYWE > Konie... > - Konie > - - Pozostałe"
            full_desc = " > ".join(new_context)
            out.append({"code": str(code).strip(), "description": full_desc})

        subgroup = node.get("subgroup")
        if isinstance(subgroup, list):
            for child in subgroup:
                _walk_tree_with_context(child, out, new_context)

    elif isinstance(node, list):
        for item in node:
            _walk_tree_with_context(item, out, context)


def _parse_codes_tree(json_data):
    out = []
    _walk_tree_with_context(json_data, out, context=[])
    return out


def build_codes_cache(date="2025-11-17", language="PL", pages=3, time_budget_seconds=25):
    global CODES_CACHE, CODES_CACHE_META

    start = time.time()
    items = []
    last_ok_page = 0

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

            if r.status_code in (404, 422):
                break

            r.raise_for_status()
            data = r.json()

            page_items = _parse_codes_tree(data)
            if not page_items:
                break

            items.extend(page_items)
            last_ok_page = page

        # deduplikacja
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
    try:
        build_codes_cache(date=date, language=language, pages=pages)
        return {"ok": True, "meta": CODES_CACHE_META}
    except Exception as e:
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
    if (
        not CODES_CACHE_META["built"]
        or CODES_CACHE_META.get("date") != date
        or CODES_CACHE_META.get("language") != language
    ):
        try:
            build_codes_cache(date=date, language=language, pages=3)
        except Exception:
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
