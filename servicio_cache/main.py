from fastapi import FastAPI, Query
import redis
import requests
import json
import time

app = FastAPI(title="Servicio de Caché")

cache = redis.Redis(host='redis_cache', port=6379, db=0, decode_responses=True)

URL_GENERADOR = "http://generador_respuestas:8000"
URL_METRICAS = "http://servicio_metricas:8002"

def procesar_cache(cache_key: str, endpoint: str, params: dict, tipo_consulta: str, start_time: float):
    """Función unificada para buscar en caché, delegar y registrar métricas."""
    cached_data = cache.get(cache_key)
    
    if cached_data:
        latencia = time.time() - start_time
        try:
            requests.post(f"{URL_METRICAS}/registrar", 
                          json={"tipo_evento": "hit", "consulta": tipo_consulta, "latencia": latencia})
        except requests.exceptions.RequestException:
            pass
        return json.loads(cached_data)
    else:
        try:
            respuesta = requests.get(f"{URL_GENERADOR}{endpoint}", params=params)
            respuesta.raise_for_status()
            datos_json = respuesta.json()
            
            cache.setex(cache_key, 60, json.dumps(datos_json))
            latencia = time.time() - start_time
            
            try:
                requests.post(f"{URL_METRICAS}/registrar", 
                              json={"tipo_evento": "miss", "consulta": tipo_consulta, "latencia": latencia})
            except requests.exceptions.RequestException:
                pass
            return datos_json
            
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

@app.get("/q1")
def cache_q1(zone_id: str, confidence_min: float = Query(0.0)):
    start_time = time.time()

    cache_key = f"count:{zone_id}:conf={confidence_min}"
    return procesar_cache(cache_key, "/q1", {"zone_id": zone_id, "confidence_min": confidence_min}, "Q1", start_time)
@app.get("/q2")
def cache_q2(zone_id: str, confidence_min: float = Query(0.0)):
    start_time = time.time()

    cache_key = f"area:{zone_id}:conf={confidence_min}"
    return procesar_cache(cache_key, "/q2", {"zone_id": zone_id, "confidence_min": confidence_min}, "Q2", start_time)

@app.get("/q3")
def cache_q3(zone_id: str, confidence_min: float = Query(0.0)):
    start_time = time.time()

    cache_key = f"density:{zone_id}:conf={confidence_min}"
    return procesar_cache(cache_key, "/q3", {"zone_id": zone_id, "confidence_min": confidence_min}, "Q3", start_time)

@app.get("/q4")
def cache_q4(zone_a: str, zone_b: str, confidence_min: float = Query(0.0)):
    start_time = time.time()

    cache_key = f"compare:density:{zone_a}:{zone_b}:conf={confidence_min}"
    return procesar_cache(cache_key, "/q4", {"zone_a": zone_a, "zone_b": zone_b, "confidence_min": confidence_min}, "Q4", start_time)

@app.get("/q5")
def cache_q5(zone_id: str, bins: int = Query(5)):
    start_time = time.time()

    cache_key = f"confidence_dist:{zone_id}:bins={bins}"
    return procesar_cache(cache_key, "/q5", {"zone_id": zone_id, "bins": bins}, "Q5", start_time)     