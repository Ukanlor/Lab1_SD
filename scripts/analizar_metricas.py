import os
import pandas as pd
import matplotlib.pyplot as plt

CSV_PATH = "servicio_metricas/metricas.csv"
OUT_DIR = "resultados"

os.makedirs(OUT_DIR, exist_ok=True)

# Escenarios que pertenecen a la Tarea 2.
ESCENARIOS_TAREA2 = [
    "kafka_1_consumer",
    "kafka_2_consumers",
    "kafka_4_consumers",
    "falla_temporal",
    "reintentos_forzados",
    "reintentos_dlq",
    "dlq_forzada",
    "spike_normal",
    "spike_alto",
    "spike_recuperacion"
]

EVENTOS_KAFKA = ["success", "recovered", "retry", "dlq"]

df = pd.read_csv(CSV_PATH)

# Normalizar nombres de columnas por si vienen con espacios.
df.columns = [c.strip() for c in df.columns]

# Convertir columnas relevantes.
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df["latencia"] = pd.to_numeric(df["latencia"], errors="coerce")
df["retry_count"] = pd.to_numeric(df["retry_count"], errors="coerce").fillna(0)

# Filtrar solo datos útiles de la Tarea 2.
df_kafka = df[
    df["scenario"].isin(ESCENARIOS_TAREA2)
    & df["tipo_evento"].isin(EVENTOS_KAFKA)
].copy()

if df_kafka.empty:
    print("No se encontraron métricas de Kafka.")
    print("Revisa que metricas.csv tenga escenarios como kafka_1_consumer, falla_temporal o dlq_forzada.")
    exit()

# Resumen por escenario.
summary = df_kafka.groupby("scenario").agg(
    total_eventos=("tipo_evento", "count"),
    exitos=("tipo_evento", lambda x: (x == "success").sum()),
    recuperadas=("tipo_evento", lambda x: (x == "recovered").sum()),
    reintentos=("tipo_evento", lambda x: (x == "retry").sum()),
    dlq=("tipo_evento", lambda x: (x == "dlq").sum()),
    latencia_p50=("latencia", "median"),
    latencia_p95=("latencia", lambda x: x.quantile(0.95)),
    inicio=("timestamp", "min"),
    fin=("timestamp", "max")
).reset_index()

summary["duracion_seg"] = (summary["fin"] - summary["inicio"]).dt.total_seconds()
summary["duracion_seg"] = summary["duracion_seg"].replace(0, 1)

summary["throughput"] = (
    summary["exitos"] + summary["recuperadas"]
) / summary["duracion_seg"]

summary["retry_rate"] = summary["reintentos"] / summary["total_eventos"].replace(0, 1)
summary["dlq_rate"] = summary["dlq"] / summary["total_eventos"].replace(0, 1)
summary["recovery_rate"] = summary["recuperadas"] / (
    summary["recuperadas"] + summary["dlq"]
).replace(0, 1)

summary.to_csv(f"{OUT_DIR}/resumen_metricas.csv", index=False)

print("\nResumen de métricas:")
print(summary)

# ---------------------------
# Gráfico 1: throughput
# ---------------------------
plt.figure()
plt.bar(summary["scenario"], summary["throughput"])
plt.xticks(rotation=45, ha="right")
plt.ylabel("Consultas exitosas por segundo")
plt.title("Throughput por escenario")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/throughput_por_escenario.png")

# ---------------------------
# Gráfico 2: latencia p50
# ---------------------------
plt.figure()
plt.bar(summary["scenario"], summary["latencia_p50"])
plt.xticks(rotation=45, ha="right")
plt.ylabel("Latencia p50")
plt.title("Latencia p50 por escenario")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/latencia_p50.png")

# ---------------------------
# Gráfico 3: latencia p95
# ---------------------------
plt.figure()
plt.bar(summary["scenario"], summary["latencia_p95"])
plt.xticks(rotation=45, ha="right")
plt.ylabel("Latencia p95")
plt.title("Latencia p95 por escenario")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/latencia_p95.png")

# ---------------------------
# Gráfico 4: retry rate
# ---------------------------
plt.figure()
plt.bar(summary["scenario"], summary["retry_rate"])
plt.xticks(rotation=45, ha="right")
plt.ylabel("Retry rate")
plt.title("Retry rate por escenario")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/retry_rate.png")

# ---------------------------
# Gráfico 5: DLQ rate
# ---------------------------
plt.figure()
plt.bar(summary["scenario"], summary["dlq_rate"])
plt.xticks(rotation=45, ha="right")
plt.ylabel("DLQ rate")
plt.title("DLQ rate por escenario")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/dlq_rate.png")

# ---------------------------
# Gráfico 6: recovery rate
# ---------------------------
plt.figure()
plt.bar(summary["scenario"], summary["recovery_rate"])
plt.xticks(rotation=45, ha="right")
plt.ylabel("Recovery rate")
plt.title("Recovery rate por escenario")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/recovery_rate.png")

# ---------------------------
# Gráfico 7: comparación de consumidores
# ---------------------------
df_consumers = summary[
    summary["scenario"].isin([
        "kafka_1_consumer",
        "kafka_2_consumers",
        "kafka_4_consumers"
    ])
].copy()

if not df_consumers.empty:
    orden = {
        "kafka_1_consumer": 1,
        "kafka_2_consumers": 2,
        "kafka_4_consumers": 4
    }

    df_consumers["num_consumers"] = df_consumers["scenario"].map(orden)
    df_consumers = df_consumers.sort_values("num_consumers")

    plt.figure()
    plt.plot(df_consumers["num_consumers"], df_consumers["throughput"], marker="o")
    plt.xlabel("Número de consumidores")
    plt.ylabel("Throughput")
    plt.title("Throughput vs número de consumidores")
    plt.xticks(df_consumers["num_consumers"])
    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/throughput_vs_consumidores.png")

    plt.figure()
    plt.plot(df_consumers["num_consumers"], df_consumers["latencia_p95"], marker="o")
    plt.xlabel("Número de consumidores")
    plt.ylabel("Latencia p95")
    plt.title("Latencia p95 vs número de consumidores")
    plt.xticks(df_consumers["num_consumers"])
    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/latencia_p95_vs_consumidores.png")

print(f"\nGráficos guardados en la carpeta: {OUT_DIR}")