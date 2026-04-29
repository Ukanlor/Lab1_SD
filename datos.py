import pandas as pd
import matplotlib.pyplot as plt
import os

# Crear carpeta para guardar los gráficos si no existe
os.makedirs("graficos", exist_ok=True)

# =====================================================================
# ¡ATENCIÓN! Cambia estas variables antes de cada nueva prueba
# =====================================================================
nombre_archivo_salida = "50MB_Random"
titulo_grafico = "50MB - Random (Alternativa FIFO)"
# =====================================================================

# 1. Leer datos asumiendo que no hay encabezado para no perder la primera fila si son datos
nombres_columnas = ["timestamp", "tipo_evento", "consulta", "latencia"]
df = pd.read_csv('servicio_metricas/metricas.csv', header=None, names=nombres_columnas)

# --- LIMPIEZA A PRUEBA DE BALAS ---
# Si la primera fila es el texto del encabezado, la eliminamos
df = df[df['timestamp'] != 'timestamp'].copy()

# Convertir tipos de datos correctamente
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
df['latencia'] = df['latencia'].astype(float)
# ----------------------------------

# 2. Calcular Throughput (Peticiones por segundo)
tiempo_total_segundos = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
total_reqs = len(df)
throughput = total_reqs / tiempo_total_segundos if tiempo_total_segundos > 0 else 0

# 3. Calcular Hit Rate y Latencias en milisegundos
hits = len(df[df['tipo_evento'] == 'hit'])
misses = len(df[df['tipo_evento'] == 'miss'])
hit_rate = hits / total_reqs if total_reqs > 0 else 0

p50 = df['latencia'].quantile(0.50) * 1000
p95 = df['latencia'].quantile(0.95) * 1000
df['latencia_ms'] = df['latencia'] * 1000

# ---------------------------------------------------------
# GRÁFICO 1: Hits vs Misses (Torta)
# ---------------------------------------------------------
plt.figure(figsize=(6, 6))
plt.pie([hits, misses], labels=['Hits', 'Misses'], autopct='%1.1f%%', colors=['#4CAF50', '#F44336'], startangle=90)
plt.title(f'Tasa de Aciertos - {titulo_grafico}')
plt.savefig(f"graficos/hit_rate_{nombre_archivo_salida}.png", dpi=300, bbox_inches='tight')
plt.close()

# ---------------------------------------------------------
# GRÁFICO 2: Distribución de Latencias
# ---------------------------------------------------------
plt.figure(figsize=(8, 5))
plt.hist(df['latencia_ms'], bins=50, color='skyblue', edgecolor='black')
plt.axvline(p50, color='red', linestyle='dashed', linewidth=1.5, label=f'p50: {p50:.2f}ms')
plt.axvline(p95, color='orange', linestyle='dashed', linewidth=1.5, label=f'p95: {p95:.2f}ms')
plt.title(f'Distribución de Latencias - {titulo_grafico}')
plt.xlabel('Latencia (ms)')
plt.ylabel('Frecuencia')
plt.legend()
plt.grid(axis='y', alpha=0.75)
plt.savefig(f"graficos/latencias_{nombre_archivo_salida}.png", dpi=300, bbox_inches='tight')
plt.close()

# ---------------------------------------------------------
# GRÁFICO 3: Latencia Promedio por Consulta
# ---------------------------------------------------------
plt.figure(figsize=(8, 5))
latencia_por_consulta = df.groupby('consulta')['latencia_ms'].mean()
latencia_por_consulta.plot(kind='bar', color='coral', edgecolor='black')
plt.title(f'Latencia Promedio por Consulta - {titulo_grafico}')
plt.xlabel('Tipo de Consulta')
plt.ylabel('Latencia Promedio (ms)')
plt.xticks(rotation=0)
plt.grid(axis='y', alpha=0.75)
plt.savefig(f"graficos/latencia_consultas_{nombre_archivo_salida}.png", dpi=300, bbox_inches='tight')
plt.close()

# ---------------------------------------------------------
# SALIDA PARA EL INFORME (Tabla LaTeX)
# ---------------------------------------------------------
print("\n" + "="*60)
print(f"RESULTADOS GENERADOS PARA: {titulo_grafico}")
print("="*60)
print(f"Total peticiones : {total_reqs}")
print(f"Tiempo de prueba : {tiempo_total_segundos:.2f} segundos")
print(f"Hit Rate         : {hit_rate:.2%}")
print(f"Throughput       : {throughput:.2f} req/s")
print(f"Latencia p50     : {p50:.2f} ms")
print(f"Latencia p95     : {p95:.2f} ms")
print("\n-> ¡3 Gráficos guardados en la carpeta 'graficos/'!")
print("-" * 60)
print("Copia esta fila directamente a tu tabla en LaTeX:")
print(f"{titulo_grafico} & {hit_rate*100:.2f}\\% & {throughput:.2f} & {p50:.2f} / {p95:.2f} & - \\\\")
print("="*60)