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
- [docs/manual_usuario.md](docs/manual_usuario.md)
- [docs/informe_resultados.md](docs/informe_resultados.md)

## Variables de entorno

Revisa `.env.example`:

- `REDIS_HOST`
- `REDIS_PORT`
- `JOB_QUEUE_HIGH_NAME`
- `JOB_QUEUE_NORMAL_NAME`
- `JOB_QUEUE_LOW_NAME`
- `JOB_PRIORITY_HIGH_MAX`
- `JOB_PRIORITY_NORMAL_MAX`
- `WORKER_QUEUES`
- `COORDINATOR_DB_PATH`
- `JOB_TIMEOUT_SECONDS`
- `JOB_MAX_RETRIES`
- `JOB_RETRY_INTERVAL_SECONDS`
- `WORKER_PROCESS_TIMEOUT_SECONDS`

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
- Las operaciones implementadas usan `ffmpeg/ffprobe` dentro del worker:
  - `extract_audio`: video -> `.mp3`
  - `generate_thumbnail`: video -> `.jpg`
  - `transcode_h264`: video -> `.mp4` (H.264 + AAC)
  - `extract_metadata`: archivo -> `.json` con metadatos tecnicos
- Para una prueba exitosa necesitas colocar en `dataset/` un video real con pista de audio; `dataset/demo.mp4` solo sirve como placeholder de estructura.
- Los resultados quedan guardados en `results/` con prefijo `{job_id}_{operation}`.
- El `docker-compose` levanta tres workers (`worker-1`, `worker-2`, `worker-3`) para evidenciar distribucion real de jobs.
- La prioridad ahora es real con colas separadas:
  - prioridad `1..3` -> `jobs_high`
  - prioridad `4..7` -> `jobs_normal`
  - prioridad `8..10` -> `jobs_low`

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
   - elegir la operacion (`extract_audio`, `generate_thumbnail`, `transcode_h264`, `extract_metadata`)
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
   - el bloque de cola muestra `jobs_high`, `jobs_normal` y `jobs_low`

## Dataset curado y metadatos

El repositorio no versiona videos pesados. Para generar un dataset reproducible
con distintos tamanos, duraciones y orientaciones:

```bash
docker compose run --rm worker_1 python scripts/generate_curated_dataset.py --dataset-dir /app/dataset
docker compose run --rm worker_1 python scripts/build_dataset_metadata.py --dataset-dir /app/dataset --output /app/dataset/dataset_metadata.json
```

El manifest queda en `dataset/dataset_metadata.json`.

## Generacion automatica por lote

Para generar tareas automaticamente desde los archivos del dataset:

```bash
python scripts/generate_batch_jobs.py --coordinator-url http://localhost:8000 --dataset-dir dataset --operation extract_audio --priority 5 --concurrency 6
```

Opciones utiles:

- Simular sin enviar jobs:

  ```bash
  python scripts/generate_batch_jobs.py --dry-run
  ```

- Repetir cada archivo varias veces (carga concurrente):

  ```bash
  python scripts/generate_batch_jobs.py --repeat 3 --concurrency 10
  ```

- Usar metadatos por archivo (prioridad/operacion por item):

  ```bash
  python scripts/generate_batch_jobs.py --metadata-json scripts/example_batch_metadata.json
  ```

## Pruebas de carga formales

Con el stack arriba y el dataset documentado:

```bash
docker compose run --rm worker_1 python scripts/run_load_test.py --coordinator-url http://coordinator:8000 --dataset-metadata /app/dataset/dataset_metadata.json --repeat 2 --concurrency 8
```

Salidas principales:

- `results/load_test_metrics.json`
- `docs/informe_resultados.md`

## Limpieza para entrega

Revisar artefactos generados y duplicados:

```bash
docker compose run --rm worker_1 python scripts/prepare_delivery_cleanup.py
```

Aplicar limpieza de `results/` solo cuando ya no necesites los artefactos de
ejecucion:

```bash
docker compose run --rm worker_1 python scripts/prepare_delivery_cleanup.py --apply
```

## Endpoints actuales del coordinador

- `POST /jobs`
- `GET /jobs`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/events`
- `GET /jobs/{job_id}/result`
