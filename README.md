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

## Endpoints actuales del coordinador

- `POST /jobs`
- `GET /jobs`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/events`
- `GET /jobs/{job_id}/result`
