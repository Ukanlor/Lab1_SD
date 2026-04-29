# Tarea 1: Plataforma de análisis de preguntas y respuestas en internet

## Integrante
- Nicolás Contreras Silva

## Arquitectura del Sistema
El sistema consta de cuatro microservicios independientes orquestados mediante contenedores:
1. **Generador de Respuestas (Puerto 8000):** Backend en FastAPI que carga los datos en memoria y procesa las consultas (Q1-Q5).
2. **Servicio de Caché (Puerto 8001):** API intermediaria que intercepta las peticiones y administra el almacenamiento temporal utilizando Redis con soporte de TTL y políticas de evicción.
3. **Servicio de Métricas (Puerto 8002):** Recolector de eventos (hits, misses, latencias) que almacena los registros en un archivo CSV persistente.
4. **Redis Cache (Puerto 6379):** Motor de base de datos en memoria para el almacenamiento de caché.

Adicionalmente, se incluye un script local (**Generador de Tráfico**) para realizar pruebas de estrés sobre el sistema.

## Requisitos Previos
Para desplegar este proyecto, asegúrese de tener instalado:
* **Docker y Docker Compose**.
* **Python 3.11+** (Para ejecutar el generador de tráfico y el extractor de datos a nivel local).

## Instrucciones de Despliegue y Ejecución

### 1. Levantar la Arquitectura
El sistema completo está contenerizado. Para iniciarlo, abra una terminal en la raíz del proyecto y ejecute:

docker compose up --build

Nota: El sistema estará listo cuando las consolas de los servicios en Docker indiquen "Application startup complete".

### 2. Ejecución Manual
Una vez que Docker esté corriendo, puede interactuar con el Servicio de Caché (que intercepta las consultas) ingresando las siguientes URLs en su navegador web:

*   **Consulta Q1 (Conteo):** `http://localhost:8001/q1?zone_id=Z1&confidence_min=0.8`
*   **Consulta Q2 (Área):** `http://localhost:8001/q2?zone_id=Z1&confidence_min=0.5`
*   **Consulta Q3 (Densidad):** `http://localhost:8001/q3?zone_id=Z3`
*   **Consulta Q4 (Comparación):** `http://localhost:8001/q4?zone_a=Z1&zone_b=Z2`
*   **Consulta Q5 (Distribución):** `http://localhost:8001/q5?zone_id=Z1&bins=5`

### 3. Ejecución Automatizada (Prueba de Estrés)
Para simular el tráfico de las empresas de logística y evaluar el rendimiento del caché bajo distribuciones Uniforme y Zipf, abra una nueva terminal en su equipo local:

1. Instale las dependencias locales del script:
   
   pip install requests numpy pandas
   
2. Ejecute el simulador:
   
   python generador_trafico/main.py
   
Este script enviará automáticamente lotes de peticiones aleatorias hacia el caché, saturando el sistema para generar métricas reales.

### 4. Extracción de Métricas y Resultados
Cada interacción con el caché (Hit o Miss) se registra en `servicio_metricas/metricas.csv`. Tras ejecutar la prueba de estrés, analice los resultados ejecutando:

python datos.py

Este script automatizado realizará tres acciones:

1) Imprimirá en consola el volumen total de peticiones, el Throughput, el Hit Rate general y los percentiles de latencia (p50 y p95).

2) Generará una cadena de texto preformateada lista para ser insertada en las tablas comparativas del informe en LaTeX.

3) Creará automáticamente una carpeta graficos/ conteniendo tres png: distribución de tasa de aciertos, histograma de latencias y latencia promedio por tipo de consulta.

### 5. Configuración de Experimentos (Caché)
Para probar distintas configuraciones requeridas en el informe (ej. 50MB vs 200MB, o LRU vs LFU):
1. Detenga el sistema con `docker compose down`.
2. Edite la directiva `command` del servicio `redis_cache` en el archivo `docker-compose.yml`.
3. Borre el archivo `metricas.csv` para limpiar los datos anteriores.
4. Vuelva a levantar el sistema con `docker compose up` y repita la prueba de estrés.

