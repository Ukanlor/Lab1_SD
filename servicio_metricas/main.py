from fastapi import FastAPI
from pydantic import BaseModel
import csv
from datetime import datetime
import os

app = FastAPI(title="Servicio de Métricas")
CSV_FILE = "metricas.csv"

class Metrica(BaseModel):
    tipo_evento: str
    consulta: str
    latencia: float
    timestamp: str = None

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "tipo_evento", "consulta", "latencia"])

@app.post("/registrar")
def registrar_metrica(m: Metrica):
    m.timestamp = datetime.now().isoformat()
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([m.timestamp, m.tipo_evento, m.consulta, m.latencia])
    return {"status": "ok"}