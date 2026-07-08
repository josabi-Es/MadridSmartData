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

## Tráfico histórico — `208627-0-transporte-ptomedida-historico`

- 159 recursos, **100% formato ZIP**, `datastore_active: false` en todos —
  no hay opción de Datastore SQL aquí, toca descargar + descomprimir cada ZIP.
- Un recurso por mes, cobertura Enero 2013 → Mayo 2026 (incluye el mes en curso).
- Nombre de recurso no indica el periodo — solo la `description` lo dice
  (ej. `"Histórico de datos del tráfico. Diciembre 2022"`), hay que parsearla.

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
