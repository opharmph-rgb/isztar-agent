from fastapi import FastAPI
import requests

app = FastAPI()

@app.get("/")
def home():
    return {"status": "ok"}

@app.get("/measures")
def measures(code: str, date: str = "2025-11-17", language: str = "PL"):
    url = "https://ext-isztar4.mf.gov.pl/tariff/rest/goods-nomenclature/measures"
    r = requests.get(
        url,
        params={
            "nomenclatureCode": code,
            "date": date,
            "language": language
        },
        timeout=30
    )
    return r.json()
