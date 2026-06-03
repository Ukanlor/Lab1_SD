from fastapi import FastAPI, Query, HTTPException
import pandas as pd
import numpy as np
import os
import random
import time

app = FastAPI(title="Generador de Respuestas")

data_en_memoria = {}

area_km2 = {
    "Z1": 14.3,   # Providencia
    "Z2": 99.4,   # Las Condes
    "Z3": 133.0,  # Maipú
    "Z4": 23.2,   # Santiago Centro
    "Z5": 197.0   # Pudahuel
}

FAIL_RATE = float(os.getenv("FAIL_RATE", "0.0"))
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.0"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.0"))


def simular_falla_temporal():
    """
    Permite simular fallas temporales o lentitud artificial.
    Se usa para probar retry, DLQ y recuperación ante fallos.
    """
    if MAX_DELAY > 0:
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    if random.random() < FAIL_RATE:
        raise HTTPException(
            status_code=503,
            detail="Falla temporal simulada en Generador de Respuestas"
        )


@app.on_event("startup")
def cargar_datos():
    """
    Carga datos sintéticos en memoria para las cinco zonas.
    Esto mantiene el mismo modelo de la Tarea 1: no hay BD en tiempo de ejecución.
    """
    global data_en_memoria

    rng = np.random.default_rng(42)
    data_en_memoria = {}

    for zone_id in ["Z1", "Z2", "Z3", "Z4", "Z5"]:
        n = 1000
        data_en_memoria[zone_id] = pd.DataFrame({
            "latitude": rng.uniform(-33.55, -33.38, n),
            "longitude": rng.uniform(-70.82, -70.55, n),
            "area_in_meters": rng.uniform(40, 300, n),
            "confidence": rng.uniform(0.3, 1.0, n)
        })

    print("Datos cargados en memoria para Z1-Z5.")


@app.get("/q1")
def q1_count(zone_id: str, confidence_min: float = Query(0.0)):
    simular_falla_temporal()

    if zone_id not in data_en_memoria:
        return {"error": "Zona no encontrada"}

    df_zona = data_en_memoria[zone_id]
    conteo = int((df_zona["confidence"] >= confidence_min).sum())

    return {
        "zone_id": zone_id,
        "count": conteo,
        "confidence_min": confidence_min
    }


@app.get("/q2")
def q2_area(zone_id: str, confidence_min: float = Query(0.0)):
    simular_falla_temporal()

    if zone_id not in data_en_memoria:
        return {"error": "Zona no encontrada"}

    df = data_en_memoria[zone_id]
    df_filtrado = df[df["confidence"] >= confidence_min]

    if df_filtrado.empty:
        return {
            "zone_id": zone_id,
            "avg_area": 0,
            "total_area": 0,
            "n": 0
        }

    return {
        "zone_id": zone_id,
        "avg_area": float(df_filtrado["area_in_meters"].mean()),
        "total_area": float(df_filtrado["area_in_meters"].sum()),
        "n": int(len(df_filtrado))
    }


@app.get("/q3")
def q3_density(zone_id: str, confidence_min: float = Query(0.0)):
    simular_falla_temporal()

    if zone_id not in data_en_memoria or zone_id not in area_km2:
        return {"error": "Zona no encontrada o sin datos de área"}

    df = data_en_memoria[zone_id]
    count = int((df["confidence"] >= confidence_min).sum())
    densidad = count / area_km2[zone_id]

    return {
        "zone_id": zone_id,
        "density": densidad,
        "confidence_min": confidence_min
    }


@app.get("/q4")
def q4_compare(
    zone_a: str,
    zone_b: str,
    confidence_min: float = Query(0.0)
):
    simular_falla_temporal()

    if zone_a not in data_en_memoria or zone_b not in data_en_memoria:
        return {"error": "Una o ambas zonas no existen"}

    df_a = data_en_memoria[zone_a]
    df_b = data_en_memoria[zone_b]

    count_a = int((df_a["confidence"] >= confidence_min).sum())
    count_b = int((df_b["confidence"] >= confidence_min).sum())

    da = count_a / area_km2[zone_a]
    db = count_b / area_km2[zone_b]

    ganador = zone_a if da > db else zone_b

    return {
        "zone_a": zone_a,
        "zone_b": zone_b,
        "density_a": da,
        "density_b": db,
        "winner": ganador,
        "confidence_min": confidence_min
    }


@app.get("/q5")
def q5_confidence_dist(zone_id: str, bins: int = Query(5)):
    simular_falla_temporal()

    if zone_id not in data_en_memoria:
        return {"error": "Zona no encontrada"}

    df = data_en_memoria[zone_id]
    scores = df["confidence"].to_numpy()
    counts, edges = np.histogram(scores, bins=bins, range=(0.0, 1.0))

    resultado = []

    for i in range(bins):
        resultado.append({
            "bucket": i,
            "min": float(edges[i]),
            "max": float(edges[i + 1]),
            "count": int(counts[i])
        })

    return {
        "zone_id": zone_id,
        "distribution": resultado
    }