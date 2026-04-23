# Proyecto Multimedia Distribuido

Plataforma distribuida para procesamiento multimedia usando Python, Docker, Redis y workers concurrentes.

Arquitectura base:

`Cliente -> Coordinador -> Cola (Redis/RQ) -> Workers -> Resultados -> Dashboard`

## Estado actual (iteracion inicial)

- Coordinador FastAPI para crear y consultar jobs.
- Cola Redis/RQ para desacoplar coordinador y workers.
- Worker consumidor de cola configurable por entorno.
- Persistencia SQLite para jobs, eventos y resultados.

## Documento de diseno tecnico

El diseno tecnico detallado (arquitectura, estados de job, contratos API, balanceo y modelo de datos) esta en:

- [docs/technical_design.md](docs/technical_design.md)

## Variables de entorno

Revisa `.env.example`:

- `REDIS_HOST`
- `REDIS_PORT`
- `JOB_QUEUE_NAME`
- `WORKER_QUEUES`
- `COORDINATOR_DB_PATH`

## Arranque minimo con Docker

1. Construir y levantar los servicios:

   ```bash
   docker compose up --build
   ```

2. Verificar que el coordinador responde:

   ```bash
   curl http://localhost:8000/
   ```

3. Crear un job de prueba:

   ```bash
   curl -X POST http://localhost:8000/jobs \
     -H "Content-Type: application/json" \
     -d "{\"file_path\":\"dataset/demo.mp4\",\"operation\":\"extract_audio\",\"priority\":5}"
   ```

4. Consultar jobs registrados:

   ```bash
   curl http://localhost:8000/jobs
   ```

Notas:

- Los resultados y la base SQLite se guardan en `results/`.
- El dataset de prueba se monta desde `dataset/`.
- La operacion implementada de forma real es `extract_audio`, usando `ffmpeg` dentro del worker.
- Para una prueba exitosa necesitas colocar en `dataset/` un video real con pista de audio; `dataset/demo.mp4` solo sirve como placeholder de estructura.
- El resultado esperado de `extract_audio` es un archivo `.mp3` guardado en `results/`.
- El `docker-compose` levanta tres workers (`worker-1`, `worker-2`, `worker-3`) para evidenciar distribucion real de jobs.

## Validacion de distribucion

1. Levanta el stack reconstruyendo imagenes:

   ```bash
   docker compose up --build
   ```

2. Envia varios jobs seguidos contra el mismo video real del dataset.

3. Consulta cada job con `GET /jobs/{job_id}` y revisa el campo `worker_id`.

4. Confirma en los logs que los tres workers arrancaron:

   ```bash
   docker compose logs worker_1 worker_2 worker_3
   ```

La validacion queda correcta cuando distintos jobs terminan con distintos valores de `worker_id`.

## Monitoreo y dashboard

- `GET /monitor/summary`: devuelve un resumen del sistema con cola, jobs, workers y metricas del coordinador.
- `GET /monitor/dataset-files`: lista los archivos disponibles en `dataset/` para crear jobs desde la interfaz.
- `GET /dashboard`: muestra una vista HTML simple con jobs recientes, workers activos y estado general.

Para validar esta fase:

1. Reconstruye y levanta el stack:

   ```bash
   docker compose up --build
   ```

2. Abre en el navegador:

   ```text
   http://localhost:8000/dashboard
   ```

   Desde esta vista ahora puedes:
   - seleccionar un archivo real del `dataset`
   - elegir la prioridad del job
   - crear el job sin usar la terminal
   - observar el tablero con refresco automatico cada 3 segundos

3. Opcionalmente consulta el resumen JSON:

   ```bash
   curl http://localhost:8000/monitor/summary
   ```

4. Envia varios jobs y confirma que:
   - aparecen en la tabla de jobs recientes
   - los workers muestran `worker-1`, `worker-2`, `worker-3`
   - el conteo por estado cambia conforme se encolan y completan trabajos

## Endpoints actuales del coordinador

- `POST /jobs`
- `GET /jobs`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/events`
- `GET /jobs/{job_id}/result`
