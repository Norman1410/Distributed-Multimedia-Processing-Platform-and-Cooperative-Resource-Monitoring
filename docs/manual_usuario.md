# Manual de usuario

Este manual resume el flujo final para levantar la plataforma, preparar el dataset,
ejecutar pruebas y diagnosticar problemas comunes.

## 1) Requisitos

- Docker Desktop en ejecucion.
- Puerto `8000` libre para el coordinador.
- Puerto `6379` libre para Redis.
- Espacio disponible en `dataset/` y `results/`.

## 2) Arranque

```bash
docker compose up --build
```

Verificaciones basicas:

```bash
curl http://localhost:8000/
curl http://localhost:8000/monitor/summary
```

Dashboard:

```text
http://localhost:8000/dashboard
```

## 3) Dataset curado

Los videos no se versionan en Git. Para generar un dataset reproducible con varios
tamanos, duraciones y orientaciones:

```bash
docker compose run --rm worker_1 python scripts/generate_curated_dataset.py --dataset-dir /app/dataset
docker compose run --rm worker_1 python scripts/build_dataset_metadata.py --dataset-dir /app/dataset --output /app/dataset/dataset_metadata.json
```

Si se ejecuta en el host y existe `ffmpeg`/`ffprobe` local:

```bash
python scripts/generate_curated_dataset.py
python scripts/build_dataset_metadata.py
```

El manifest queda en:

```text
dataset/dataset_metadata.json
```

## 4) Crear jobs manualmente

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d "{\"file_path\":\"dataset/curated_01_short_360p.mp4\",\"operation\":\"extract_metadata\",\"priority\":5}"
```

Consultar estado:

```bash
curl http://localhost:8000/jobs
curl http://localhost:8000/jobs/{job_id}
curl http://localhost:8000/jobs/{job_id}/events
curl http://localhost:8000/jobs/{job_id}/result
```

Operaciones soportadas:

- `extract_metadata`
- `generate_thumbnail`
- `extract_audio`
- `transcode_h264`

## 5) Prueba de carga formal

Con stack levantado y dataset generado:

```bash
docker compose run --rm worker_1 python scripts/run_load_test.py \
  --coordinator-url http://coordinator:8000 \
  --dataset-metadata /app/dataset/dataset_metadata.json \
  --repeat 2 \
  --concurrency 8
```

Salidas:

- `results/load_test_metrics.json`: metricas completas en JSON.
- `docs/informe_resultados.md`: informe legible con tiempos, throughput,
  distribucion por worker y fallos.

Para una prueba mas fuerte:

```bash
docker compose run --rm worker_1 python scripts/run_load_test.py \
  --coordinator-url http://coordinator:8000 \
  --dataset-metadata /app/dataset/dataset_metadata.json \
  --repeat 5 \
  --concurrency 12 \
  --max-wait-seconds 3600
```

## 6) Endpoints principales

- `GET /`: salud basica y operaciones soportadas.
- `POST /jobs`: crea job.
- `GET /jobs`: lista jobs.
- `GET /jobs/{job_id}`: estado de un job.
- `GET /jobs/{job_id}/events`: bitacora del job.
- `GET /jobs/{job_id}/result`: resultado persistido.
- `GET /monitor/summary`: resumen de colas, workers y metricas.
- `GET /monitor/dataset-files`: archivos disponibles para dashboard.
- `GET /monitor/operations`: operaciones disponibles.
- `GET /dashboard`: interfaz web.

## 7) Robustez operativa

Variables relevantes:

- `JOB_TIMEOUT_SECONDS`: timeout de RQ por job.
- `JOB_MAX_RETRIES`: reintentos maximos gestionados por RQ.
- `JOB_RETRY_INTERVAL_SECONDS`: espera entre reintentos.
- `WORKER_PROCESS_TIMEOUT_SECONDS`: timeout para comandos `ffmpeg`/`ffprobe`.

El coordinador registra `attempt_count`, `max_attempts`, `error_type` y
`retryable` en cada job. Los eventos permiten distinguir errores de entrada,
operacion no soportada, timeout, herramienta no disponible y fallos multimedia.

## 8) Troubleshooting

- `docker compose ps`: confirma que Redis, coordinador y workers estan arriba.
- `docker compose logs coordinator`: errores al encolar o exponer API.
- `docker compose logs worker_1 worker_2 worker_3`: errores de ffmpeg, timeouts
  y reintentos.
- `curl http://localhost:8000/monitor/summary`: revisa workers activos y colas.
- Si no aparecen archivos en dashboard, revisa que `dataset/` tenga videos reales
  y que no solo exista `.gitkeep`.
- Si un job falla con `input_file_not_found`, verifica que el `file_path` sea del
  estilo `dataset/nombre.mp4`.
- Si hay timeouts, aumenta `JOB_TIMEOUT_SECONDS` y `WORKER_PROCESS_TIMEOUT_SECONDS`
  o reduce la concurrencia de la prueba.

## 9) Limpieza para entrega

Revisar sin borrar:

```bash
docker compose run --rm worker_1 python scripts/prepare_delivery_cleanup.py
```

Limpiar artefactos generados en `results/`:

```bash
docker compose run --rm worker_1 python scripts/prepare_delivery_cleanup.py --apply
```

Detectar duplicados exactos en el dataset:

```bash
docker compose run --rm worker_1 python scripts/prepare_delivery_cleanup.py --remove-dataset-duplicates
```

La entrega debe conservar:

- codigo fuente
- `README.md`
- `docs/technical_design.md`
- `docs/manual_usuario.md`
- `docs/informe_resultados.md`
- `dataset/dataset_metadata.json`
- `.gitkeep` en `dataset/` y `results/`
