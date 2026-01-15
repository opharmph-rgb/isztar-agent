from fastapi import FastAPI
import requests

app = FastAPI()

BASE = "https://ext-isztar4.mf.gov.pl/tariff/rest/goods-nomenclature"

@app.get("/")
def home():
    return {"status": "ok"}

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
def search_codes(q: str, date: str = "2025-11-17", language: str = "PL", pages: int = 3):
    """
    MVP: pobiera kilka stron listy kodów z ISZTAR4 i filtruje po opisie.
    pages=3 oznacza: sprawdź page=1..3 (możesz zwiększyć).
    """
    q_low = q.lower().strip()
    found = []

    for page in range(1, max(1, pages) + 1):
        url = f"{BASE}/codes"
        r = requests.get(url, params={"date": date, "language": language, "page": page}, timeout=30)
        r.raise_for_status()
        data = r.json()

        # API może zwracać listę lub obiekt z "results" — obsługujemy oba
        rows = data if isinstance(data, list) else data.get("results", [])

        for row in rows:
            code = str(row.get("code", "")).strip()
            desc = str(row.get("description", "")).strip()
            if not code or not desc:
                continue
            if q_low in desc.lower():
                found.append({"code": code, "description": desc})

    # Usuń duplikaty po code
    unique = {}
    for item in found:
        unique[item["code"]] = item

    items = list(unique.values())[:50]  # limit
    return {"query": q, "count": len(items), "items": items}
