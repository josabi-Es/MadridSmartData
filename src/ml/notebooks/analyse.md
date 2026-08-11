# analyse — column inventory of `bronze`, `silver` and `gold`

Flat-text companion to `eda_bronze.ipynb` / `eda_silver.ipynb` /
`eda_gold.ipynb`: for every table in `data/bronze/`, `data/silver/` and
`data/gold/`, what columns it has, what type each column is, and 5 sample
rows. No charts, no modelling conclusions — this is a "get to know the data"
reference.

Generated 2026-08-01 by reading the parquet files directly (pandas for the
small ones, DuckDB over the parquet for `trafico`, never loading it whole).

> **Note:** this replaces an earlier version of this file. Silver and gold
> were rewritten this session (medallion renaming refactor, see
> `src/data/gold/_catalog.yml` for the authoritative schema doc going
> forward): every column is now UPPERCASE and homogenized project-wide
> (`ID_AIRE`, `ID_TRAFICO`, `COD_DIS`), several gold tables were trimmed or
> restructured, and `DIM_FECHA` is new. The saved outputs inside
> `eda_bronze.ipynb`/`eda_silver.ipynb`/`eda_gold.ipynb` were **not**
> re-executed and still show the old schema — don't trust them, this file is
> the current source of truth. Bronze itself is untouched (raw ingestion,
> no renaming there — see "Where the data comes from" below).

## Where the data comes from

Five CKAN resources from the Madrid open-data portal, landed in
`data/bronze/` one file per year (air) or per month (traffic), each folder
carrying a `manifest.json` with the resource id and the row count it
ingested:

| bronze folder | CKAN resource | layout |
|---|---|---|
| `aire/` | `201410-*-calidad-aire-diario-csv` | `2023.parquet`, `2024.parquet`, `2025.parquet` (this one also holds 2026 rows) |
| `estaciones_aire/` | `212629-0-estaciones-control-aire-csv` | `latest.parquet` |
| `distritos/` | `300497-2-distritos-municipales-madrid` | `latest.parquet` |
| `trafico/` | `208627-*-transporte-ptomedida-historico-zip` | `2023-01`, `2023-02`, `2023-5`, `2024-01`, `2024-02`, `2024-12`, `2025-12` |
| `trafico_puntos_medida/` | `202468-*-intensidad-trafico-csv` | `2023-01`, `2023-5`, `2024-12`, `2025-12` |

Traffic is **not** a continuous history — only 7 months were ever
downloaded, spread across three years (2023-01, 2023-02, 2023-05, 2024-01,
2024-02, 2024-12, 2025-12). `min(FECHA)`/`max(FECHA)` span 2023-01-01 to
2025-12-31, which looks like three continuous years but isn't — 29 of the
36 months in between don't exist. Air quality has no such gap: complete and
daily from 2023-01-01 to 2026-06-30.

---

## Silver

Bronze → silver renames every column UPPERCASE and homogenizes keys
project-wide (`ID_AIRE`, `ID_TRAFICO`, `COD_DIS` with leading zero) — this
is new this session, silver used to keep bronze's original mixed casing.

### `aire` — `data/silver/aire.parquet`

157.741 rows × 5 columns · 2023-01-01 → 2026-06-30 · 24 stations · 11
magnitudes · `VALIDEZ` is `V` in 100% of rows (bronze → silver already
drops everything but valid readings).

| column | dtype | note |
|---|---|---|
| `ID_AIRE` | `int64` | short station code (`4`, `8`, `11`…) — was `estacion`/`ESTACION`, joins to `estaciones_aire.ID_AIRE` |
| `MAGNITUD` | `object` (str) | label, not the numeric code — `NO2`, `O3`, `PM10`… |
| `FECHA` | `object` (`datetime.date`) | native `DATE` in the parquet; pandas materializes it as `object` unless you `pd.to_datetime(...)` |
| `DATO` | `float64` | daily value |
| `VALIDEZ` | `object` (str) | always `V` here |

```
 ID_AIRE MAGNITUD       FECHA  DATO VALIDEZ
       4       NO  2024-01-31  62.0       V
       4       NO  2024-08-31   2.0       V
       8      TOL  2024-08-31   0.7       V
       8      BEN  2024-05-31   0.1       V
      11      NOx  2024-01-31  79.0       V
```

### `estaciones_aire` — `data/silver/estaciones_aire.parquet`

24 rows × 27 columns. Same shape as bronze (nothing dropped at this layer —
trimming happens in gold) plus `COD_DIS`/`NOMBRE` from the spatial join,
renamed uppercase, `CODIGO_CORTO`→`ID_AIRE`, `"Fecha alta"`→`FECHA_ALTA`.
**`COD_DIS` changed meaning this session**: it now comes from bronze's
`COD_DIS_TX` (zero-padded, `"09"`) instead of `COD_DIS` (`"9"`) — this is
the project's one and only district-code representation from here on. All
24 stations still match a district, 0 nulls.

| column | dtype | note |
|---|---|---|
| `CODIGO` | `int64` | long code, `28079004` (kept in silver, dropped in gold) |
| `ID_AIRE` | `int64` | short code — was `CODIGO_CORTO` |
| `ESTACION`, `DIRECCION` | `object` | name, address |
| `LONGITUD_ETRS89` / `LATITUD_ETRS89` | `object` | DMS string, not numeric |
| `ALTITUD` | `int64` | metres |
| `COD_TIPO` / `NOM_TIPO` | `object` | `UT`/`Urbana tráfico`, `UF`/`Urbana fondo` |
| `NO2`, `SO2`, `CO`, `PM10`, `PM2_5`, `O3`, `BTX` | `object` | `"X"` if measured, `None` otherwise (still text here — gold converts to 1/0) |
| `COD_VIA`, `VIA_CLASE`, `VIA_PAR`, `VIA_NOMBRE` | street reference |
| `FECHA_ALTA` | `datetime64[us]` | commissioning date — was `"Fecha alta"` |
| `COORDENADA_X_ETRS89` / `_Y_ETRS89` | `object` | comma-decimal string, not numeric |
| `LONGITUD` / `LATITUD` | `float64` | clean coordinates |
| `COD_DIS` / `NOMBRE` | `object` | district code (zero-padded) / name, from the spatial join |

```
 ID_AIRE          ESTACION    LATITUD  LONGITUD COD_DIS NO2   SO2    CO  PM10 PM2_5    O3   BTX
       4   Plaza de España  40.423882 -3.712257      09   X     X     X  None  None  None  None
       8  Escuelas Aguirre  40.421553 -3.682316      04   X     X     X     X     X     X     X
      11     Ramón y Cajal  40.451473 -3.677349      05   X  None  None  None  None  None     X
      16      Arturo Soria  40.440046 -3.639242      15   X  None  None  None  None     X  None
      17        Villaverde  40.347147 -3.713317      17   X  None  None  None  None     X  None
```
*(other columns elided from the sample for width.)*

### `trafico` — `data/silver/trafico.parquet`

89.234.783 rows × 9 columns · 15-minute granularity · 4.884 distinct
`ID_TRAFICO` · 658 MB. Never `pd.read_parquet` this one — query with DuckDB.

**New this session:** rows with `ERROR != 'N'` are now dropped here (general
project rule: only valid, error-free readings survive silver). That's why
`ERROR` is `'N'` in 100% of the remaining rows, and why gold's
`FACT_TRAFICO` can drop the `ERROR` column entirely — it would be constant.
Bronze → silver lost 197.180 rows to this filter (89.431.963 → 89.234.783,
~0,22%), on top of the pre-existing negative/NaN cleaning.

| column | type (DuckDB) | note |
|---|---|---|
| `ID_TRAFICO` | `BIGINT` | measuring point — was `id` |
| `FECHA` | `TIMESTAMP` | 15-minute slot |
| `TIPO_ELEM` | `VARCHAR` | `URB`, `M30`, `C30`… sensor type |
| `INTENSIDAD` | `DOUBLE` | vehicles/hour |
| `OCUPACION` | `DOUBLE` | % of time occupied |
| `CARGA` | `DOUBLE` | load 0-100 |
| `VMED` | `DOUBLE` | mean speed km/h (M30 sensors only) |
| `ERROR` | `VARCHAR` | always `N` now |
| `PERIODO_INTEGRACION` | `BIGINT` | minutes aggregated into the row |

```
┌────────────┬─────────────────────┬───────────┬────────────┬───┬────────┬────────┬─────────┬─────────────────────┐
│ ID_TRAFICO │        FECHA        │ TIPO_ELEM │ INTENSIDAD │ … │ CARGA  │  VMED  │  ERROR  │ PERIODO_INTEGRACION │
├────────────┼─────────────────────┼───────────┼────────────┼───┼────────┼────────┼─────────┼─────────────────────┤
│       1001 │ 2023-01-02 08:15:00 │ C30       │      300.0 │ … │    0.0 │   56.0 │ N       │                   5 │
│       1001 │ 2023-01-03 07:15:00 │ C30       │      300.0 │ … │    0.0 │   56.0 │ N       │                   5 │
│       1001 │ 2023-01-06 08:15:00 │ C30       │      528.0 │ … │    0.0 │   62.0 │ N       │                   5 │
│       1001 │ 2023-01-13 16:30:00 │ C30       │     1992.0 │ … │    0.0 │   59.0 │ N       │                   5 │
│       1001 │ 2023-01-13 20:15:00 │ C30       │     2436.0 │ … │    0.0 │   62.0 │ N       │                   5 │
└────────────┴─────────────────────┴───────────┴────────────┴───┴────────┴────────┴─────────┴─────────────────────┘
```

---

## Gold

Rewritten this session into a Power BI-ready star schema: every table
uppercase, homogenized keys (`ID_AIRE`, `ID_TRAFICO`, `COD_DIS`) so
relationships can be inferred by column name, several dimensions trimmed to
just what's needed, and a new `DIM_FECHA`. Authoritative schema doc going
forward: `src/data/gold/_catalog.yml`.

### `DIM_DISTRITO` — 22 rows × 11 columns (21 real districts + 1 sentinel)

| column | dtype | note |
|---|---|---|
| `COD_DIS` | `object` | district key, zero-padded (`"09"`) — this is bronze's old `COD_DIS_TX`, now the project standard |
| `NOMBRE`, `DISTRI_MAY`, `DISTRI_MT` | `object` | name variants |
| `AREA` | `float64` | m² |
| `GEOMETRY` | `geometry` (WKB) | polygon, ETRS89/UTM30N |
| `GEOMETRY_WKT` | `object` (str) | **new** — same polygon as WKT text, for Power BI custom visuals that can't read the WKB `GEOMETRY` column directly (e.g. Icon Map) |
| `N_ESTACIONES_AIRE`, `N_PUNTOS_TRAFICO` | `int64` | coverage counters |
| `COBERTURA_AIRE` | `bool` | `N_ESTACIONES_AIRE > 0` |
| `COBERTURA_TRAFICO` | `bool` | **new** — `N_PUNTOS_TRAFICO > 0` |

**New sentinel row** `COD_DIS = '-1'`, `NOMBRE = 'Sin distrito asignado'`
(all other columns 0/False/NULL): the handful of traffic points/readings
with no real district in the source now point here instead of leaving a
NULL foreign key — the classic Kimball "unknown member" fix, so Power BI
never renders a blank relationship row.

```
 COD_DIS        NOMBRE  N_ESTACIONES_AIRE  N_PUNTOS_TRAFICO COBERTURA_AIRE COBERTURA_TRAFICO
      -1  Sin distrito…                   0                 5          False              False
      01        Centro                   1               166           True               True
      02    Arganzuela                   1               259           True               True
      03        Retiro                   1               171           True               True
      04     Salamanca                   1               217           True               True
```
5 districts still have `COBERTURA_AIRE = False` (Tetuán, Latina, Usera,
Vicálvaro, San Blas-Canillejas) — same 5 as before this session, unchanged
by the refactor. Every district has `COBERTURA_TRAFICO = True`.

### `DIM_ESTACION_AIRE` — 24 rows × 13 columns

Trimmed from silver's 27 columns down to what a Power BI model needs, and
the gas flags converted from text to binary.

| column | dtype | note |
|---|---|---|
| `ID_AIRE` | `int64` | key |
| `ESTACION`, `DIRECCION` | `object` | name, address |
| `LATITUD`, `LONGITUD` | `float64` | clean coordinates |
| `COD_DIS` | `object` | district, zero-padded |
| `NO2`, `SO2`, `CO`, `PM10`, `PM2_5`, `O3`, `BTX` | `int64` | **binary now** — 1 if the station measures it, 0 if not (was `"X"`/`None`) |

Dropped vs. silver: `CODIGO` (long code), `LONGITUD_ETRS89`/`LATITUD_ETRS89`,
`ALTITUD`, `COD_TIPO`/`NOM_TIPO`, `COD_VIA`/`VIA_*`, `FECHA_ALTA`,
`COORDENADA_X/Y_ETRS89`, district `NOMBRE` (redundant — already reachable
via `COD_DIS` → `DIM_DISTRITO.NOMBRE`).

```
 ID_AIRE          ESTACION    LATITUD  LONGITUD COD_DIS  NO2  SO2  CO  PM10  PM2_5  O3  BTX
       4   Plaza de España  40.423882 -3.712257      09    1    1   1     0      0   0    0
       8  Escuelas Aguirre  40.421553 -3.682316      04    1    1   1     1      1   1    1
      11     Ramón y Cajal  40.451473 -3.677349      05    1    0   0     0      0   0    1
      16      Arturo Soria  40.440046 -3.639242      15    1    0   0     0      0   1    0
      17        Villaverde  40.347147 -3.713317      17    1    0   0     0      0   1    0
```

### `DIM_MAGNITUD` — 18 rows × 2 columns

| column | dtype | note |
|---|---|---|
| `ID_MAGNITUD` | `int32` | numeric magnitude code — was `codigo` |
| `MAGNITUD` | `object` | label — was `magnitud` |

Only 11 of these 18 appear in `FACT_CALIDAD_AIRE`; the rest are catalogue
entries with no readings in this data range.

```
 ID_MAGNITUD MAGNITUD
           1      SO2
           6       CO
           7       NO
           8      NO2
           9    PM2.5
```

### `DIM_PUNTO_TRAFICO` — 5.081 rows × 5 columns

Trimmed from 9 columns down to 5; grain is clean (one row per `ID_TRAFICO`).

| column | dtype | note |
|---|---|---|
| `ID_TRAFICO` | `int64` | key — was `id` |
| `COD_DIS` | `object` | zero-padded; sentinel `'-1'` for the 5 points with no district (was `NULL`) |
| `NOMBRE` | `object` | descriptive location |
| `LATITUD`, `LONGITUD` | `float64` | WGS84 |

Dropped vs. bronze/old gold: `tipo_elem`, `cod_cent`, `utm_x`, `utm_y`.

```
 ID_TRAFICO COD_DIS      NOMBRE    LATITUD  LONGITUD
       1001      10  05FT10PM01  40.409729 -3.740786
       1002      10  05FT37PM01  40.408030 -3.743760
       1003      10  05FT66PM01  40.406824 -3.746834
       1006      10  04FT74PM01  40.411894 -3.736324
       1009      09  03FT52PM01  40.416234 -3.724909
```

### `DIM_FECHA` — 1.277 rows × 6 columns (new this session)

One row per calendar day, spanning the combined date range of both facts
(2023-01-01 → 2026-06-30). Built from silver directly (not from the facts)
to avoid a build-order dependency.

| column | dtype | note |
|---|---|---|
| `FECHA` | `datetime64` (midnight) | join key — day grain |
| `ANIO`, `MES` | `int64` | |
| `NOMBRE_MES` | `object` | e.g. `"January"` |
| `DIA_SEMANA` | `object` | e.g. `"Sunday"` |
| `ES_FIN_SEMANA` | `bool` | Sat/Sun |

`FACT_TRAFICO` joins to this on `CAST(FECHA AS DATE)` since its own `FECHA`
keeps full 15-minute resolution — hour-level breakdowns (`HORA`,
`FRANJA_HORARIA`, `ES_HORA_PICO`) are deliberately **not** built here: they're
trivial calculated columns in Power BI (`HOUR([FECHA])` + a `SWITCH`), no
need to persist or version them in the pipeline.

```
      FECHA  ANIO  MES NOMBRE_MES DIA_SEMANA  ES_FIN_SEMANA
 2023-01-01  2023    1    January     Sunday           True
 2023-01-02  2023    1    January     Monday          False
 2023-01-03  2023    1    January    Tuesday          False
 2023-01-04  2023    1    January  Wednesday          False
 2023-01-05  2023    1    January   Thursday          False
```

### `FACT_CALIDAD_AIRE` — 157.741 rows × 5 columns

Same row count as `silver/aire` — nothing lost in this join, just narrowed
and re-keyed. Grain: `ID_AIRE` × `FECHA` × `ID_MAGNITUD`, unique (157.741
distinct keys for 157.741 rows).

| column | dtype | note |
|---|---|---|
| `FECHA` | `DATE` | |
| `ID_AIRE` | `BIGINT` | |
| `COD_DIS` | `VARCHAR` | zero-padded |
| `ID_MAGNITUD` | `INTEGER` | **new** — numeric FK to `DIM_MAGNITUD`, replaces the `MAGNITUD` text column entirely (smaller for VertiPaq, and lets Power BI infer the relationship) |
| `DATO` | `DOUBLE` | |

`VALIDEZ` is gone — silver already keeps only valid readings, so it would
be constant here.

```
┌────────────┬─────────┬─────────┬─────────────┬────────┐
│   FECHA    │ ID_AIRE │ COD_DIS │ ID_MAGNITUD │  DATO  │
├────────────┼─────────┼─────────┼─────────────┼────────┤
│ 2024-05-31 │       4 │ 09      │           7 │    3.0 │
│ 2024-10-31 │       4 │ 09      │           7 │    8.0 │
│ 2024-01-31 │       8 │ 04      │           7 │   26.0 │
│ 2024-07-31 │       8 │ 04      │          10 │   44.0 │
│ 2024-08-31 │       8 │ 04      │          14 │   73.0 │
└────────────┴─────────┴─────────┴─────────────┴────────┘
```

### `FACT_TRAFICO` — 89.228.903 rows × 7 columns

Same 7 months as silver · 15-minute grain · 4.875 distinct `ID_TRAFICO`.
Query with DuckDB only.

| column | dtype | note |
|---|---|---|
| `FECHA` | `TIMESTAMP` | full 15-min timestamp, kept for hour-level PBI analysis |
| `ID_TRAFICO` | `BIGINT` | |
| `COD_DIS` | `VARCHAR` | zero-padded; **98.781 rows now carry the `'-1'` sentinel** instead of a NULL |
| `INTENSIDAD`, `OCUPACION`, `CARGA`, `VMED` | `DOUBLE` | |

`ERROR` is gone — silver already keeps only `ERROR='N'` rows, so it would
be constant here. Silver → gold loses 5.880 rows (89.234.783 → 89.228.903):
the handful of points that don't exist in `DIM_PUNTO_TRAFICO`'s snapshot
union (inner join), not the same thing as the sentinel rows above (those
*do* have a matching point, just no district on it).

```
┌─────────────────────┬────────────┬─────────┬────────────┬───────────┬────────┬────────┐
│        FECHA        │ ID_TRAFICO │ COD_DIS │ INTENSIDAD │ OCUPACION │ CARGA  │  VMED  │
├─────────────────────┼────────────┼─────────┼────────────┼───────────┼────────┼────────┤
│ 2024-01-03 11:45:00 │      10118 │ 15      │      504.0 │       4.0 │   29.0 │    0.0 │
│ 2024-01-06 04:15:00 │      10118 │ 15      │       14.0 │       0.0 │    1.0 │    0.0 │
│ 2024-01-10 00:15:00 │      10118 │ 15      │      114.0 │       2.0 │    5.0 │    0.0 │
│ 2024-01-10 20:15:00 │      10118 │ 15      │      551.0 │       7.0 │   34.0 │    0.0 │
│ 2024-01-11 04:15:00 │      10118 │ 15      │       22.0 │       0.0 │    0.0 │    0.0 │
└─────────────────────┴────────────┴─────────┴────────────┴───────────┴────────┴────────┘
```

---

## Star schema

```
DIM_MAGNITUD ─ID_MAGNITUD→ FACT_CALIDAD_AIRE ←ID_AIRE─ DIM_ESTACION_AIRE
                                   │                          │
                                   └──────────COD_DIS─────────┴──→ DIM_DISTRITO ←─COD_DIS──┐
                                                                                            │
                            FACT_TRAFICO ←ID_TRAFICO─ DIM_PUNTO_TRAFICO ───COD_DIS──────────┘
                                   │
                          CAST(FECHA AS DATE)
                                   │
                                   ▼
                              DIM_FECHA ←──────────────── FACT_CALIDAD_AIRE.FECHA
```

**`FACT_CALIDAD_AIRE`**: `ID_AIRE` → `DIM_ESTACION_AIRE.ID_AIRE` ·
`COD_DIS` → `DIM_DISTRITO.COD_DIS` · `ID_MAGNITUD` → `DIM_MAGNITUD.ID_MAGNITUD`
· `FECHA` → `DIM_FECHA.FECHA` (direct, already day-grain).

**`FACT_TRAFICO`**: `ID_TRAFICO` → `DIM_PUNTO_TRAFICO.ID_TRAFICO` ·
`COD_DIS` → `DIM_DISTRITO.COD_DIS` · `CAST(FECHA AS DATE)` → `DIM_FECHA.FECHA`
(own `FECHA` keeps 15-min resolution, no separate day column).

**Every FK is non-null** now: `COD_DIS` always resolves to a real
`DIM_DISTRITO` row, including the sentinel `'-1'` for unassigned points.

---

## Bronze

Unchanged this session — bronze is raw ingestion (`csv_to_parquet`/
`shapefile_to_parquet` pass source headers straight through, no renaming),
and stays that way on purpose: nothing downstream reads bronze directly
except silver/gold, and keeping it untouched preserves "this is exactly
what the API returned." See the previous version of this analysis (or
`eda_bronze.ipynb`, outputs stale but the schema itself hasn't moved) for
full bronze column tables — the short version: `aire` bronze is wide
(`D01..D31`/`V01..V31`, numeric `MAGNITUD` code), `estaciones_aire`/
`distritos`/`trafico_puntos_medida` are the direct source of silver's/gold's
uppercase columns before renaming, and `trafico` bronze has the same 9
columns as silver before the `ERROR='N'` filter and uppercase rename.

---

## Not covered here

`data/gold/ml/` (`PRED_<gas>_<N>m.parquet` — note now uppercase columns
too: `FECHA, ID_AIRE, COD_DIS, ID_MAGNITUD, VALOR_PREDICHO, MODELO`, no more
`HORIZONTE_MESES` since it's already in the filename — plus
`metrics_<gas>_<N>m.json`) is the output of `python -m src.ml.main`, not
part of the star schema, so it stays out of scope for this inventory.
