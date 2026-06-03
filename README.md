# Tarea 2: Procesamiento y Fallback con Apache Kafka

## Integrante
- Nicolás Contreras Silva

## Descripción General

Este proyecto corresponde a la segunda entrega del sistema distribuido de análisis de consultas geoespaciales sobre edificaciones en la Región Metropolitana de Santiago.

La arquitectura original de la Tarea 1 utilizaba un flujo síncrono compuesto por:

1. Generador de Tráfico
2. Servicio de Caché
3. Generador de Respuestas
4. Servicio de Métricas
5. Redis

En esta segunda entrega se incorpora **Apache Kafka** como sistema de mensajería para desacoplar la generación de consultas del procesamiento. Además, se implementan mecanismos de **reintentos**, **Dead Letter Queue (DLQ)**, procesamiento con **múltiples consumidores** y medición de **backlog**.

---

## Arquitectura del Sistema

La arquitectura implementada queda compuesta por los siguientes servicios:

1. **Apache Kafka**
   - Broker de mensajería.
   - Recibe consultas nuevas en el tópico principal.
   - Permite desacoplar el productor de los consumidores.

2. **Generador de Tráfico**
   - Genera consultas Q1-Q5.
   - Utiliza distribución uniforme o Zipf.
   - Publica las consultas en Kafka.
   - Cada consulta incluye:
     - `id`
     - `query_type`
     - `params`
     - `created_at`
     - `retry_count`
     - `scenario`

3. **Consumidor Kafka**
   - Consume mensajes desde Kafka.
   - Pertenece al grupo de consumo `query-processors`.
   - Consulta inicialmente el Servicio de Caché.
   - Si ocurre una falla, envía el mensaje a retry.
   - Si se supera el número máximo de reintentos, envía el mensaje a la DLQ.

4. **Servicio de Caché**
   - Implementado con FastAPI.
   - Usa Redis como almacenamiento temporal.
   - Si hay cache hit, retorna la respuesta directamente.
   - Si hay cache miss, consulta al Generador de Respuestas.

5. **Generador de Respuestas**
   - Procesa consultas Q1-Q5.
   - Carga datos en memoria.
   - Permite simular fallas temporales mediante variables de entorno:
     - `FAIL_RATE`
     - `MIN_DELAY`
     - `MAX_DELAY`

6. **Servicio de Métricas**
   - Registra métricas en `servicio_metricas/metricas.csv`.
   - Guarda eventos como:
     - `success`
     - `recovered`
     - `retry`
     - `dlq`
     - `hit`
     - `miss`

7. **Redis**
   - Sistema de almacenamiento en memoria para caché.
   - Se configura con límite de memoria y política de remoción.

---

## Tópicos Kafka

El sistema utiliza tres tópicos principales:

`queries` Recibe consultas nuevas generadas por el Generador de Tráfico
`retry-queries` Recibe consultas que fallaron temporalmente y deben reprocesarse
`dead-letter-queue` Almacena consultas que superaron el máximo de reintentos

---

## Requisitos Previos

Para ejecutar el proyecto se requiere:

- Docker Desktop
- Docker Compose
- Python 3.11+
- Apache Kafka, levantado mediante Docker Compose
- Dependencias locales para análisis:
  - pandas
  - matplotlib

Instalación de dependencias locales:

pip install pandas matplotlib


---

## Despliegue del Sistema

Desde la raíz del proyecto, ejecutar:

docker compose down
docker compose up -d --build kafka generador_respuestas servicio_metricas redis_cache servicio_cache


Luego levantar los consumidores Kafka:

docker compose up -d --build --scale consumidor_kafka=1 consumidor_kafka


Verificar servicios:

docker compose ps


---

## Creación de Tópicos Kafka

Verificar tópicos existentes:

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list


Crear tópicos si no existen:

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic queries --partitions 3 --replication-factor 1


docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic retry-queries --partitions 3 --replication-factor 1


docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic dead-letter-queue --partitions 3 --replication-factor 1


---

## Ejecución del Generador de Tráfico

Ejemplo con 1 consumidor:

docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=kafka_1_consumer -e NUM_PETICIONES=1000 -e RATE=50 -e DISTRIBUTION=uniforme generador_trafico


Ver logs del consumidor:

docker compose logs -f consumidor_kafka


Para salir de los logs en vivo:

Ctrl + C


---

## Escenarios de Evaluación

### 1. Kafka + 1 Consumer

docker compose down
docker compose up -d --build kafka generador_respuestas servicio_metricas redis_cache servicio_cache
docker compose up -d --build --scale consumidor_kafka=1 consumidor_kafka
docker compose exec redis_cache redis-cli FLUSHALL


docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=kafka_1_consumer -e NUM_PETICIONES=1000 -e RATE=50 -e DISTRIBUTION=uniforme generador_trafico


---

### 2. Kafka + 2 Consumers

docker compose up -d --build --scale consumidor_kafka=2 consumidor_kafka
docker compose exec redis_cache redis-cli FLUSHALL


docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=kafka_2_consumers -e NUM_PETICIONES=1000 -e RATE=100 -e DISTRIBUTION=uniforme generador_trafico


---

### 3. Kafka + 4 Consumers

docker compose up -d --build --scale consumidor_kafka=4 consumidor_kafka
docker compose exec redis_cache redis-cli FLUSHALL


docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=kafka_4_consumers -e NUM_PETICIONES=1000 -e RATE=100 -e DISTRIBUTION=uniforme generador_trafico


---

### 4. Falla Temporal del Generador de Respuestas

Primero levantar el sistema con consumidores:

docker compose up -d --build --scale consumidor_kafka=2 consumidor_kafka
docker compose exec redis_cache redis-cli FLUSHALL


Detener el Generador de Respuestas:

docker compose stop generador_respuestas


Enviar tráfico:

docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=falla_temporal -e NUM_PETICIONES=100 -e RATE=20 -e DISTRIBUTION=uniforme generador_trafico


Revisar logs:

docker compose logs -f consumidor_kafka


Se deberían observar eventos:

[ERROR]
[RETRY]


Luego levantar nuevamente el Generador de Respuestas:

docker compose up -d generador_respuestas


En los logs deberían aparecer consultas recuperadas:

[OK] query_id=... retry=1


---

### 5. DLQ Forzada

Para forzar el envío a DLQ, modificar temporalmente en `docker-compose.yml`:

FAIL_RATE: "1.0"

Y opcionalmente reducir:

MAX_RETRIES: "2"

Reiniciar servicios:

docker compose down
docker compose up -d --build kafka generador_respuestas servicio_metricas redis_cache servicio_cache
docker compose up -d --build --scale consumidor_kafka=1 consumidor_kafka
docker compose exec redis_cache redis-cli FLUSHALL


Ejecutar tráfico:

docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=dlq_forzada -e NUM_PETICIONES=50 -e RATE=10 -e DISTRIBUTION=uniforme generador_trafico


Revisar logs:

docker compose logs -f consumidor_kafka


Se deberían observar eventos:

[DLQ] query_id=... enviada a DLQ


Revisar mensajes en la DLQ:

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dead-letter-queue --from-beginning --timeout-ms 10000


Después de esta prueba, volver a dejar:

FAIL_RATE: "0.0"
MAX_RETRIES: "3"


---

### 6. Spike de Tráfico

Levantar sistema normal:

docker compose down
docker compose up -d --build kafka generador_respuestas servicio_metricas redis_cache servicio_cache
docker compose up -d --build --scale consumidor_kafka=2 consumidor_kafka
docker compose exec redis_cache redis-cli FLUSHALL


Tráfico normal:

docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=spike_normal -e NUM_PETICIONES=500 -e RATE=50 -e DISTRIBUTION=uniforme generador_trafico

Spike de tráfico:

docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=spike_alto -e NUM_PETICIONES=1500 -e RATE=300 -e DISTRIBUTION=uniforme generador_trafico


Recuperación:

docker compose run --rm -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e SCENARIO=spike_recuperacion -e NUM_PETICIONES=500 -e RATE=50 -e DISTRIBUTION=uniforme generador_trafico


---

## Medición de Backlog

El backlog se mide usando el `LAG` del grupo de consumidores `query-processors`.

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group query-processors


La columna relevante es:

LAG


Interpretación:

* `LAG = 0`: no hay mensajes pendientes.
* `LAG > 0`: existen mensajes pendientes en Kafka.

---

## Métricas Registradas

El sistema registra métricas en:

servicio_metricas/metricas.csv


Columnas principales:

| Columna        | Descripción                                                  |
| -------------- | ------------------------------------------------------------ |
| `timestamp`    | Momento en que se registra el evento                         |
| `scenario`     | Escenario experimental                                       |
| `query_id`     | Identificador único de la consulta                           |
| `tipo_evento`  | Evento registrado: success, recovered, retry, dlq, hit, miss |
| `consulta`     | Tipo de consulta Q1-Q5                                       |
| `latencia`     | Tiempo total de respuesta                                    |
| `retry_count`  | Número de reintentos realizados                              |
| `consumer_id`  | Identificador del consumidor Kafka                           |
| `cache_status` | Estado asociado a caché                                      |

---

## Generación de Gráficos

Para analizar los resultados:

python scripts/analizar_metricas.py


El script genera la carpeta:

resultados/


Con archivos como:

resumen_metricas.csv
throughput_por_escenario.png
latencia_p50.png
latencia_p95.png
retry_rate.png
dlq_rate.png
recovery_rate.png
throughput_vs_consumidores.png
latencia_p95_vs_consumidores.png


---

## Métricas Analizadas

Las métricas principales son:

| Métrica       | Descripción                                         |
| ------------- | --------------------------------------------------- |
| Throughput    | Consultas exitosas por segundo                      |
| Latencia p50  | Percentil 50 de latencia                            |
| Latencia p95  | Percentil 95 de latencia                            |
| Retry rate    | Proporción de consultas reenviadas a retry          |
| Recovery rate | Proporción de consultas recuperadas exitosamente    |
| DLQ rate      | Proporción de consultas enviadas a DLQ              |
| Backlog size  | Cantidad de mensajes pendientes en Kafka            |
| Recovery time | Tiempo necesario para vaciar la cola tras una falla |

---

## Limpieza de Métricas

Antes de ejecutar experimentos oficiales, se recomienda guardar una copia de las métricas antiguas y limpiar el archivo:

Copy-Item .\servicio_metricas\metricas.csv .\servicio_metricas\metricas_backup.csv -ErrorAction SilentlyContinue
Remove-Item .\servicio_metricas\metricas.csv -ErrorAction SilentlyContinue


Luego reiniciar servicios:

docker compose down
docker compose up -d --build kafka generador_respuestas servicio_metricas redis_cache servicio_cache


---

## Consultas Soportadas

El sistema mantiene las consultas de la Tarea 1:

| Consulta | Descripción                                 |
| -------- | ------------------------------------------- |
| Q1       | Conteo de edificios en una zona             |
| Q2       | Área promedio y área total de edificaciones |
| Q3       | Densidad de edificaciones por km²           |
| Q4       | Comparación de densidad entre dos zonas     |
| Q5       | Distribución de confianza en una zona       |

---

## Decisiones de Diseño

### Uso de Kafka

Kafka se incorporó como intermediario entre el Generador de Tráfico y el procesamiento de consultas. Esto permite desacoplar la llegada de solicitudes del procesamiento, evitando que las consultas se pierdan inmediatamente cuando el Generador de Respuestas falla temporalmente.

### Tópicos de Retry

Las consultas que fallan temporalmente se reenvían al tópico `retry-queries`. Cada mensaje conserva su identificador original y aumenta el contador `retry_count`.

### Dead Letter Queue

Si una consulta supera el número máximo de reintentos, se envía al tópico `dead-letter-queue`. Esto permite separar las consultas que no pudieron resolverse y analizarlas posteriormente.

### Múltiples Consumidores

Los consumidores pertenecen al mismo grupo de consumo `query-processors`. Esto permite que Kafka distribuya automáticamente la carga entre consumidores.

### Caché

La caché se mantiene como configuración base del sistema. Redis permite reducir latencia en consultas repetidas mediante almacenamiento temporal y política de remoción.

---

## Comandos Útiles

Ver servicios:

docker compose ps


Ver logs del consumidor:

docker compose logs -f consumidor_kafka


Ver logs del Generador de Respuestas:

docker compose logs -f generador_respuestas


Limpiar Redis:

docker compose exec redis_cache redis-cli FLUSHALL


Detener Generador de Respuestas:

docker compose stop generador_respuestas


Levantar Generador de Respuestas:

docker compose up -d generador_respuestas


Ver tópicos Kafka:

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

Ver backlog:

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group query-processors


---

## Resultados Esperados

Al ejecutar los escenarios experimentales se espera observar:

1. Al aumentar consumidores, el throughput mejora hasta alcanzar un cuello de botella.
2. Durante fallas temporales, las consultas se envían a retry en vez de perderse.
3. Cuando el Generador de Respuestas vuelve a estar disponible, las consultas pueden recuperarse.
4. Si las fallas persisten, las consultas terminan en la DLQ.
5. Durante spikes de tráfico, el backlog aumenta temporalmente y luego disminuye.
6. Los reintentos reducen pérdida de consultas, pero aumentan la latencia total.

---

## Archivos Principales

generador_trafico/
generador_respuestas/
servicio_cache/
servicio_metricas/
consumidor_kafka/
scripts/
resultados/
docker-compose.yml
README.md


---

## Estado Final

El sistema implementa:

* Publicación de consultas en Kafka.
* Procesamiento asíncrono mediante consumidores Kafka.
* Reintentos automáticos.
* Dead Letter Queue.
* Escalamiento horizontal con múltiples consumidores.
* Medición de backlog mediante LAG.
* Registro de métricas experimentales.
* Generación de gráficos comparativos.

---

Antes de subirlo, cambia el archivo `README.md` por este contenido y luego haz:

git add README.md
git commit -m "Actualiza README para Tarea 2 con Kafka, retry y DLQ"
git push


Y revisa que en `docker-compose.yml` hayas vuelto a dejar:

FAIL_RATE: "0.0"
MAX_RETRIES: "3"


para que el repo quede en modo normal, no en modo DLQ forzada.
