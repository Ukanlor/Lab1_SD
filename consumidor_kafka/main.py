import json
import os
import socket
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

MAIN_TOPIC = os.getenv("MAIN_TOPIC", "queries")
RETRY_TOPIC = os.getenv("RETRY_TOPIC", "retry-queries")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dead-letter-queue")
GROUP_ID = os.getenv("GROUP_ID", "query-processors")

CACHE_URL = os.getenv("CACHE_URL", "http://servicio_cache:8001")
METRICS_URL = os.getenv("METRICS_URL", "http://servicio_metricas:8002")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BASE_RETRY_DELAY = int(os.getenv("BASE_RETRY_DELAY", "5"))

CONSUMER_ID = os.getenv("CONSUMER_ID", socket.gethostname())


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

consumer = KafkaConsumer(
    MAIN_TOPIC,
    RETRY_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def calcular_latencia(query):
    created_at = parse_time(query["created_at"])
    now = datetime.now(timezone.utc)
    return (now - created_at).total_seconds()


def registrar_metrica(query, tipo_evento, latencia, cache_status="none"):
    payload = {
        "tipo_evento": tipo_evento,
        "consulta": query.get("query_type", "unknown").upper(),
        "latencia": latencia,
        "query_id": query.get("id"),
        "retry_count": int(query.get("retry_count", 0)),
        "consumer_id": CONSUMER_ID,
        "scenario": query.get("scenario", "default"),
        "cache_status": cache_status
    }

    try:
        requests.post(f"{METRICS_URL}/registrar", json=payload, timeout=2)
    except requests.exceptions.RequestException:
        pass


def endpoint_y_params(query):
    q = query["query_type"].lower()
    params = query["params"]

    if q in ["q1", "q2", "q3"]:
        return f"/{q}", {
            "zone_id": params["zone_id"],
            "confidence_min": params.get("confidence_min", 0.8)
        }

    if q == "q4":
        return "/q4", {
            "zone_a": params["zone_a"],
            "zone_b": params["zone_b"],
            "confidence_min": params.get("confidence_min", 0.8)
        }

    if q == "q5":
        return "/q5", {
            "zone_id": params["zone_id"],
            "bins": params.get("bins", 5)
        }

    raise ValueError(f"Tipo de consulta desconocido: {q}")


def enviar_a_retry_o_dlq(query):
    retry_count = int(query.get("retry_count", 0)) + 1
    query["retry_count"] = retry_count

    latencia = calcular_latencia(query)

    if retry_count <= MAX_RETRIES:
        delay = BASE_RETRY_DELAY * (2 ** (retry_count - 1))
        query["available_at"] = time.time() + delay

        producer.send(RETRY_TOPIC, query)
        producer.flush()

        registrar_metrica(query, "retry", latencia)

        print(
            f"[RETRY] query_id={query['id']} "
            f"retry={retry_count}/{MAX_RETRIES} delay={delay}s"
        )
    else:
        producer.send(DLQ_TOPIC, query)
        producer.flush()

        registrar_metrica(query, "dlq", latencia)

        print(f"[DLQ] query_id={query['id']} enviada a DLQ")


def esperar_si_es_retry(query):
    available_at = query.get("available_at")

    if available_at is None:
        return

    wait_time = float(available_at) - time.time()

    if wait_time > 0:
        print(f"[BACKOFF] Esperando {wait_time:.2f}s antes de reprocesar")
        time.sleep(wait_time)


def procesar_query(query):
    endpoint, params = endpoint_y_params(query)

    response = requests.get(
        f"{CACHE_URL}{endpoint}",
        params=params,
        timeout=5
    )

    response.raise_for_status()
    data = response.json()

    if isinstance(data, dict) and "error" in data:
        raise Exception(data["error"])

    latencia = calcular_latencia(query)
    retry_count = int(query.get("retry_count", 0))

    if retry_count > 0:
        evento = "recovered"
    else:
        evento = "success"

    registrar_metrica(query, evento, latencia)

    print(
        f"[OK] query_id={query['id']} "
        f"tipo={query['query_type']} retry={retry_count}"
    )


def main():
    print(f"Consumidor Kafka iniciado: {CONSUMER_ID}")
    print(f"Tópicos: {MAIN_TOPIC}, {RETRY_TOPIC}")
    print(f"Grupo de consumo: {GROUP_ID}")

    for message in consumer:
        query = message.value

        try:
            if message.topic == RETRY_TOPIC:
                esperar_si_es_retry(query)

            procesar_query(query)
            consumer.commit()

        except Exception as e:
            print(f"[ERROR] query_id={query.get('id')} error={e}")
            enviar_a_retry_o_dlq(query)
            consumer.commit()


if __name__ == "__main__":
    main()