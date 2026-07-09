# findings.md — 001-descubrimiento-catalogo

Confirmado en vivo contra `https://datos.madrid.es/api/3/action/*` (CKAN estándar,
`success: true`). Script usado: `src/data/ingest/catalog_probe.py`.

## Calidad del aire — `201410-0-calidad-aire-diario`

- 76 recursos, uno por año (2001-2024) en 4 formatos (CSV, XML, TXT, JSON) más un
  grupo "Desde 2025" que se actualiza de forma continua (cache_last_updated
  coincide con la fecha de hoy).
- **CSV tiene `datastore_active: true`** — se puede leer vía Datastore SQL o
  descargar el fichero directo. Optamos por descarga directa del CSV (más simple,
  mismo resultado, sin depender de `datastore_search_sql`).
- XML/TXT/JSON: `datastore_active: false`, solo descarga de fichero.
- Un recurso PDF con la definición de campos (`interprete_ficheros...pdf`).

## Estaciones de control de aire — `212629-0-estaciones-control-aire`

- 4 recursos: CSV (`datastore_active: true`, metadatos de estación:
  código/nombre/lat/lon), XLS, PDF (estructura), GEO.
- Columnas confirmadas: `CODIGO, CODIGO_CORTO, ESTACION, DIRECCION,
  LONGITUD_ETRS89, LATITUD_ETRS89, ALTITUD, COD_TIPO, NOM_TIPO, NO2, SO2, CO,
  PM10, PM2_5, O3, BTX, COD_VIA, VIA_CLASE, VIA_PAR, VIA_NOMBRE, Fecha alta,
  COORDENADA_X_ETRS89, COORDENADA_Y_ETRS89, LONGITUD, LATITUD`.
- **No trae `distrito`** — a diferencia de los puntos de medida de tráfico.
  **Sí hace falta el spatial join con `geopandas`** (mismo mecanismo que usa
  `third_tab.py` legacy: `Point(LONGITUD, LATITUD)` contra el polígono de
  distrito de `300497-0-distritos-municipales-madrid`). Encoding UTF-8, sin
  problema aquí.

## Tráfico histórico — `208627-0-transporte-ptomedida-historico`

- 159 recursos, **100% formato ZIP**, `datastore_active: false` en todos —
  no hay opción de Datastore SQL aquí, toca descargar + descomprimir cada ZIP.
- Un recurso por mes, cobertura Enero 2013 → Mayo 2026 (incluye el mes en curso).
- Nombre de recurso no indica el periodo — solo la `description` lo dice
  (ej. `"Histórico de datos del tráfico. Diciembre 2022"`), hay que parsearla.

## Puntos de medida de tráfico (metadatos) — `202468-0-intensidad-trafico`

- 298 recursos, snapshots mensuales desde 2014, formato CSV/XLSX/ZIP.
- Campos confirmados: `tipo_elem, distrito, id, cod_cent, nombre, utm_x, utm_y,
  longitud, latitud` — **trae `distrito` directo por sensor**, y `id` es la misma
  clave que usa el histórico de tráfico.
- **Implicación:** no hace falta `geopandas`/spatial join para mapear
  tráfico→distrito (a diferencia de las estaciones de aire, que si no traen
  distrito en su metadata sí lo necesitan) — join directo por `id`.
- **Encoding: `latin1`, no UTF-8** — falla con `pandas.read_csv` por defecto
  (columna `nombre` tiene tildes). Probar encoding explícito en los otros 3
  datasets también, no asumir UTF-8 en ninguno.

## Doc oficial del histórico de tráfico (PDF `208627-81-...`)

- Tipos originales: todo `Entero` salvo `fecha` (`dd/mm/yyyy hh:mi:ss`) y
  `tipo_elem`/`error` (texto).
- **"Valor negativo implica ausencia de datos"** — pero en el CSV real
  (probado con enero 2024) también aparece el literal de texto `"NaN"` como
  sentinel distinto (28.657 en `ocupacion`, 7.167 en `vmed`, 0 en
  `intensidad`/`carga` ese mes). **Dos representaciones de "sin dato"
  conviven en el mismo fichero** — la limpieza (Silver) debe tratar ambas
  como missing, no solo una.
- **`carga` es 0 (vacía) a 100 (colapso)** — corrige el rango `0-86` que
  documentaba el código legacy (`CLAUDE.md`), que estaba mal. Usar 0-100 de
  la fuente oficial.
- `vmed` solo aplica a puntos M30 (interurbanos) — en puntos urbanos su
  ausencia es estructural, no un error de sensor.
- `error`: `N` sin error, `E` alguna muestra con calidad no óptima integrada
  igualmente, `S` alguna muestra totalmente errónea y no integrada.

## Distritos

- La referencia del geoportal (`geoportal.madrid.es/IDEAM_WBGEOPORTAL/dataset.iam?id=541f4ef6-762b-11e9-861d-ecb1d753f6e`)
  **está caída** ("Error en la aplicación").
- Alternativa encontrada en el mismo CKAN: `300497-0-distritos-municipales-madrid`
  (21 distritos, límites 1987 con ajustes 2015/2020). Formatos: SHP (`Distritos.zip`),
  KML, CSV, XLSX. Sin GeoJSON nativo, pero `geopandas` (ya en `pyproject.toml`) lee
  Shapefile directamente sin conversión previa.
- **Decisión:** usar `300497-0-distritos-municipales-madrid` vía CKAN en vez del
  geoportal — mismo mecanismo de descarga que aire/tráfico, reproducible con un
  comando (cumple el criterio de éxito de `mission.md`).

## Frecuencia real

- Aire: el recurso "Desde 2025" se actualiza con frecuencia alta (no mensual
  como documentaba el portal antiguo — el `cache_last_updated` está al día).
- Tráfico: mensual, un ZIP nuevo cada mes, con el mes en curso ya disponible.

## Implicación para 002 (ingesta)

- Aire y estaciones: descarga directa de CSV, streaming con `pandas.read_csv`.
- Tráfico: descarga de ZIP + extracción antes de leer el CSV interno — el
  ingestor necesita un paso de descompresión que aire no necesita.
- Distritos: descarga de SHP + lectura con `geopandas.read_file()` (ya soportado).
- Ninguno de los tres necesita `datastore_search_sql` — descarga de fichero
  cubre todos los casos, simplifica el cliente CKAN (no hace falta implementar
  la rama de Datastore).
