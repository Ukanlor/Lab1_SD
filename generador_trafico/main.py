import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import requests
from kafka import KafkaProducer

URL_CACHE = os.getenv("URL_CACHE", "http://127.0.0.1:8001")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "queries")

MODE = os.getenv("MODE", "kafka")  # kafka o sync
SCENARIO = os.getenv("SCENARIO", "default")
DISTRIBUTION = os.getenv("DISTRIBUTION", "uniforme")
NUM_PETICIONES = int(os.getenv("NUM_PETICIONES", "500"))
RATE = int(os.getenv("RATE", "50"))

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
TIPOS_CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]


def elegir_zona(distribucion):
    if distribucion == "zipf":
        a = 2.0
        indice = np.random.zipf(a) - 1

        if indice >= len(ZONAS):
            indice = len(ZONAS) - 1

        return ZONAS[indice]

    return random.choice(ZONAS)


def construir_query(distribucion):
    zona = elegir_zona(distribucion)
    consulta = random.choice(TIPOS_CONSULTAS)

    params = {}

    if consulta in ["q1", "q2", "q3"]:
        params = {
            "zone_id": zona,
            "confidence_min": 0.8
        }

    elif consulta == "q4":
        zona_b = random.choice([z for z in ZONAS if z != zona])
        params = {
            "zone_a": zona,
            "zone_b": zona_b,
            "confidence_min": 0.8
        }

    elif consulta == "q5":
        params = {
            "zone_id": zona,
            "bins": 5
        }

    return {
        "id": str(uuid.uuid4()),
        "query_type": consulta,
        "params": params,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "retry_count": 0,
        "distribution": distribucion,
        "scenario": SCENARIO
    }


def enviar_sync(query):
    q = query["query_type"]
    params = query["params"]

    try:
        requests.get(f"{URL_CACHE}/{q}", params=params, timeout=5)
    except requests.exceptions.RequestException as e:
        print(f"[SYNC ERROR] {e}")


def enviar_kafka(producer, query):
    producer.send(TOPIC, query)


def simular_trafico():
    print("\n--- Generador de Tráfico ---")
    print(f"Modo: {MODE}")
    print(f"Escenario: {SCENARIO}")
    print(f"Distribución: {DISTRIBUTION}")
    print(f"Peticiones: {NUM_PETICIONES}")
    print(f"Rate: {RATE} consultas/s")

    producer = None

    if MODE == "kafka":
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    intervalo = 1 / RATE if RATE > 0 else 0

    for i in range(NUM_PETICIONES):
        query = construir_query(DISTRIBUTION)

        if MODE == "sync":
            enviar_sync(query)
        else:
            enviar_kafka(producer, query)

        if (i + 1) % 100 == 0:
            print(f"Enviadas {i + 1}/{NUM_PETICIONES}")

        time.sleep(intervalo)

    if producer:
        producer.flush()

    print("Simulación terminada.")


if __name__ == "__main__":
    simular_trafico()