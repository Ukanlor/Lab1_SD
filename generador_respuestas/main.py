from fastapi import FastAPI, Query
import pandas as pd
import numpy as np

app = FastAPI(title="Generador de Respuestas")

data_en_memoria = {}

#Las areas aproximadas en km2 de las zonas de santiago (para Q3)
area_km2 = {
    "Z1": 14.3,  # Providencia
    "Z2": 99.4,  # Las Condes
    "Z3": 133.0, # Maipú
    "Z4": 23.2,  # Santiago Centro
    "Z5": 197.0  # Pudahuel
}

@app.on_event("startup")
def cargar_datos():
    """
    Se ejecuta una sola vez al iniciar el contenedor
    Carga el dataset en memoria según las zonas
    """
    global data_en_memoria
    data_en_memoria = {
        "Z1": pd.DataFrame({
            "latitude": [-33.430, -33.435],
            "longitude": [-70.610, -70.615],
            "area_in_meters": [120.5, 85.0],
            "confidence": [0.9, 0.6]
        }),
        "Z2": pd.DataFrame({"confidence": [0.8, 0.4, 0.95]}),
    }
    print("Datos cargados en memoria exitosamente.")

@app.get("/q1")
def q1_count(zone_id: str, confidence_min: float = Query(0.0)):
    """
    Q1: Cuenta el número total de edificaciones detectadas dentro de una zona[cite: 1].
    """
    if zone_id not in data_en_memoria:
        return {"error": "Zona no encontrada"}
    
    df_zona = data_en_memoria[zone_id]

    conteo = int((df_zona['confidence'] >= confidence_min).sum())
    
    return {"zone_id": zone_id, "count": conteo, "confidence_min": confidence_min}

@app.get("/q2")
def q2_area(zone_id: str, confidence_min: float = Query(0.0)):
    """Q2: Calcula el área promedio y el área total construida en una zona."""
    if zone_id not in data_en_memoria:
        return {"error": "Zona no encontrada"}
    
    df = data_en_memoria[zone_id]

    df_filtrado = df[df['confidence'] >= confidence_min]
    
    if df_filtrado.empty:
         return {"avg_area": 0, "total_area": 0, "n": 0}

    avg_area = float(df_filtrado['area_in_meters'].mean())
    total_area = float(df_filtrado['area_in_meters'].sum())
    n = len(df_filtrado)
    
    return {"avg_area": avg_area, "total_area": total_area, "n": n}

@app.get("/q3")
def q3_density(zone_id: str, confidence_min: float = Query(0.0)):
    """Q3: Densidad de edificaciones por km² en una zona."""
    if zone_id not in data_en_memoria or zone_id not in area_km2:
        return {"error": "Zona no encontrada o sin datos de área"}
    
    df = data_en_memoria[zone_id]
    count = int((df['confidence'] >= confidence_min).sum())
    
    area_total = area_km2[zone_id]
    densidad = count / area_total
    
    return {"zone_id": zone_id, "density": densidad}

@app.get("/q4")
def q4_compare(zone_a: str, zone_b: str, confidence_min: float = Query(0.0)):
    """Q4: Compara la densidad de edificaciones entre dos zonas."""
    res_a = q3_density(zone_a, confidence_min)
    res_b = q3_density(zone_b, confidence_min)
    
    if "error" in res_a or "error" in res_b:
        return {"error": "Una o ambas zonas no existen"}
    
    da = res_a["density"]
    db = res_b["density"]
    
    ganador = zone_a if da > db else zone_b
    
    return {"zone_a": da, "zone_b": db, "winner": ganador}

@app.get("/q5")
def q5_confidence_dist(zone_id: str, bins: int = Query(5)):
    """Q5: Distribución de confianza en una zona usando un histograma."""
    if zone_id not in data_en_memoria:
         return {"error": "Zona no encontrada"}
    
    df = data_en_memoria[zone_id]
    scores = df['confidence'].to_numpy()
    
    counts, edges = np.histogram(scores, bins=bins, range=(0.0, 1.0))
    
    resultado = []
    for i in range(bins):
        resultado.append({
            "bucket": i,
            "min": float(edges[i]),
            "max": float(edges[i+1]),
            "count": int(counts[i])
        })
        
    return {"distribution": resultado}