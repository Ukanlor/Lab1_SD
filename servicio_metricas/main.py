from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import csv
from datetime import datetime
import os

app = FastAPI(title="Servicio de Métricas")

CSV_FILE = "metricas.csv"


class Metrica(BaseModel):
    tipo_evento: str
    consulta: str
    latencia: float
    query_id: Optional[str] = None
    retry_count: int = 0
    consumer_id: Optional[str] = None
    scenario: Optional[str] = "default"
    cache_status: Optional[str] = "none"
    timestamp: Optional[str] = None


HEADER = [
    "timestamp",
    "scenario",
    "query_id",
    "tipo_evento",
    "consulta",
    "latencia",
    "retry_count",
    "consumer_id",
    "cache_status"
]


def asegurar_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(HEADER)


asegurar_csv()


@app.post("/registrar")
def registrar_metrica(m: Metrica):
    m.timestamp = datetime.now().isoformat()

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            m.timestamp,
            m.scenario,
            m.query_id,
            m.tipo_evento,
            m.consulta,
            m.latencia,
            m.retry_count,
            m.consumer_id,
            m.cache_status
        ])

    return {"status": "ok"}