![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)

# MadridSmartData

Madrid open data turned into a governed lakehouse, a forecasting model and dashboards, all on Databricks.

## Objective

- Predict district air quality seven days ahead.
- Measure how much traffic really explains pollution.
- Madrid restricts traffic when NO₂ rises. A forecast makes that a plan, not a reaction.

## What it enables

- **Decide from a dashboard**, not from a spreadsheet. Business users read the forecast and the history in the same view.
- **Process data at scale**: 890 million rows over six years, in one query surface.
- **Combine different sources**: traffic sensors, air stations, districts and calendar, joined on shared keys.
- **Refresh on a schedule**: a weekly load keeps every dashboard current with no manual step.
- **Act seven days ahead**: a forecast per district turns a traffic restriction into a planned decision.
- **Trust the number**: gaps in the source data are logged and excluded, never silently filled.
- **Reload without fear**: re-running a month replaces it instead of duplicating it, and old years backfill on demand.
- **Map it**: district geometry is loaded and conformed, so the same facts render on a map.
- **Keep the best model only**: a new candidate is promoted only when it beats the one in production.
- **Bring your own BI**: the gold layer is open Delta, so Power BI reads it live without an export.
- **Separate dev from production**: the same code runs against two catalogs, selected by a parameter.
- **Extend it**: a new gas, district or dataset is a parameter or a config row, not a new pipeline.

## Databricks components

- **Spark**: 890 M rows, too large for single-node pandas.
- **Delta Lake**: `replaceWhere` makes re-running a month idempotent.
- **Unity Catalog**: one catalog per environment, lineage, PK constraints, model registry.
- **Volumes**: landing zone for raw CKAN files.
- **Workflows**: scheduled DAG, parameterised per gas and district.
- **MLflow**: experiment tracking and the `champion` alias.
- **Lakeview**: three dashboards over the same tables.
- **SQL Warehouse**: DirectQuery for Power BI.
- **Serverless**: no cluster to size.

### Databricks 

Demonstration of the Databricks platform components including Workflows, Unity Catalog, MLflow, and Lakeview dashboards.

<video src="https://github.com/user-attachments/assets/dabb1c5d-3bba-4c65-b295-d57c6536e9a8" controls width="700"></video>

*Direct link: [Databricks Demo Video](https://github.com/user-attachments/assets/dabb1c5d-3bba-4c65-b295-d57c6536e9a8)*

## Sources

Public CKAN API of [datos.madrid.es](https://datos.madrid.es), no manual downloads. Datasets are **config, not code**: a seed row in `infra.datasets` drives the loader.

- `aire` (csv/year)
- `trafico` (zip/month)
- `trafico_puntos_medida` (csv/month)
- `estaciones_aire`
- `distritos` (shapefile to EPSG:4326)

### Gold: galaxy schema

<img src="docs/assest/galaxy_schema.png" alt="Galaxy schema: two fact tables sharing conformed dimensions" width="670" />

Several Databricks Workflows handle load and ML, one per concern. See [`resources/jobs`](resources/jobs) for the full definitions.


### Power BI Walkthrough

Demonstration of external Power BI reporting connected directly to the Gold Delta layer via Databricks SQL Warehouse.

<video src="https://github.com/user-attachments/assets/08369aae-342b-4591-9dfe-5168fde3cac4" controls width="700"></video>

*Direct link: [Power BI Walkthrough Video](https://github.com/user-attachments/assets/08369aae-342b-4591-9dfe-5168fde3cac4)*
