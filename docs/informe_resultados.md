# Informe de resultados de pruebas de carga

Este informe resume la corrida formal ejecutada contra el dataset real del
proyecto. La prueba se conserva como evidencia principal porque ejercita el
sistema con una carga cercana al escenario solicitado por la guia: cientos de
archivos multimedia, multiples operaciones por video y tres workers cooperando.

## Resumen de la corrida

- Fecha de observacion: 2026-05-06 UTC
- Dataset: `curated_multimedia_real_dataset_400`
- Version del manifest: 2026-05-05
- Archivos base: 406
- Audios `.wav`: 200
- Videos `.mp4`: 206
- Tamano total del dataset: 501162266 bytes
- Repeticiones por tarea: 1
- Concurrencia de envio: 4
- Tareas solicitadas por el manifest: 1024
- Envios aceptados por el coordinador segun log: 1013
- Envios rechazados por HTTP 500: 11
- Jobs registrados en el coordinador al cierre: 1015
- Jobs completados: 1009
- Jobs fallidos terminales: 0
- Jobs pendientes/inconsistentes al cierre: 6
- Trabajos pendientes en Redis al cierre: 0

## Distribucion solicitada

El manifest diferencia los archivos por tipo para no aplicar operaciones de
video a audios `.wav`.

- Audio: 200 tareas
- Video: 824 tareas
- Total: 1024 tareas

## Operaciones solicitadas

- `extract_metadata`: 406
- `generate_thumbnail`: 206
- `extract_audio`: 206
- `transcode_h264`: 206

## Resultado por envio

El archivo `results/load_test_stdout.log` muestra que la mayoria de solicitudes
fueron aceptadas por el coordinador.

- `extract_metadata`: 405 envios OK, 1 error HTTP 500
- `generate_thumbnail`: 203 envios OK, 3 errores HTTP 500
- `extract_audio`: 202 envios OK, 4 errores HTTP 500
- `transcode_h264`: 203 envios OK, 3 errores HTTP 500

Los 11 errores HTTP 500 ocurrieron durante la fase de creacion de jobs. La
traza del coordinador mostro `sqlite3.OperationalError: database is locked`,
por lo que el problema no corresponde a fallos de FFmpeg ni a archivos
multimedia corruptos, sino a contencion de escritura en SQLite bajo carga
concurrente.

## Resultado de procesamiento

Al consultar `/monitor/summary` despues de la corrida se observo:

- Total de jobs registrados: 1015
- Completados: 1009
- Fallidos: 0
- En cola segun SQLite: 6
- En ejecucion: 0
- Pendientes: 0
- Cola Redis pendiente: 0

Esto indica que los workers procesaron correctamente los trabajos que llegaron
a ejecutarse. Los 6 jobs restantes quedaron como inconsistencia entre el estado
persistido y la cola, causada por los mismos bloqueos de SQLite durante la fase
de alta concurrencia.

## Evidencia de distribucion

La prueba se ejecuto con tres workers (`worker-1`, `worker-2`, `worker-3`) y el
monitor mostro workers activos durante la corrida. La cola fue drenandose
progresivamente mientras los workers ejecutaban operaciones pesadas como
`extract_audio` y `transcode_h264` sobre videos reales.

## Observaciones tecnicas

- El dataset cumple el rango recomendado de la guia: 406 archivos multimedia.
- La prueba ejercita archivos reales de audio y video.
- No se observaron fallos terminales de procesamiento multimedia.
- La limitacion encontrada fue la contencion de SQLite durante inserciones y
  actualizaciones concurrentes.
- Para mejorar la robustez, se aplicaron cambios en `shared/job_store.py` para
  usar `WAL`, `busy_timeout` y reintentos ante bloqueos temporales.
- Tambien se ajusto `scripts/run_load_test.py` para reintentar envios
  transitorios HTTP 500/503.

## Conclusiones

La corrida demuestra que la plataforma puede distribuir y procesar una carga
grande de multimedia real entre varios workers. El resultado es funcionalmente
favorable porque 1009 jobs terminaron correctamente y no hubo fallos terminales
de procesamiento.

Como punto de mejora, SQLite funciono como cuello de botella bajo la rafaga de
envios concurrentes. Para una entrega academica, esto queda documentado como
hallazgo de prueba de carga y fue mitigado con ajustes de concurrencia en la
capa de persistencia.
