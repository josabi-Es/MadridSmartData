
## Instructions

## Folder Structure

```
data/
├── raw/
│   ├── air_quality_data/        # Daily air quality records since 2001
│   ├── air_quality_sensors/     # Metadata about air quality stations
│   ├── traffic_data/            # Historical traffic measurements since 2013
│   └── traffic_sensors/         # Metadata for traffic sensors
│
├── processed/
│   ├── air_quality_processed/   # Air quality data after preprocessing (Parquet)
│   ├── traffic_data_processed/  # Traffic data after preprocessing (Parquet)
│   └── districts/               # District boundaries (GeoJSON, unchanged)
```

### 1. Download the data from the official sources:

- Air Quality Records: https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=aecb88a7e2b73410VgnVCM2000000c205a0aRCRD
- Traffic Historical Data: https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=33cb30c367e78410VgnVCM1000000b205a0aRCRD
- Air Quality Station Metadata: https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=9e42c176313eb410VgnVCM1000000b205a0aRCRD
- Traffic Sensor Metadata: https://datos.madrid.es/portal/site/egob/menuitem.c05c1f754a33a9fbe4b2e4b284f1a5a0/?vgnextoid=ee941ce6ba6d3410VgnVCM1000000b205a0aRCRD
- District Boundaries GeoJSON: https://geoportal.madrid.es/IDEAM_WBGEOPORTAL/dataset.iam?id=541f4ef6-762b-11e9-861d-ecb1d753f6e8

### 2. Convert CSV files to Parquet:

After downloading each dataset in CSV format, convert it to Parquet for better storage and processing efficiency using the scripts in `src/Preprocessing`.

### 3. Add Parquet files to the corresponding folder:

Place each converted Parquet file in the appropriate folder under `data/processed/`. The district GeoJSON file does not need to be converted to Parquet and remains under `data/processed/districts/`.

### 4. (Optional) Keep raw data:

You can also store the original CSV/GeoJSON files in `data/raw/` to have a backup of the original data.

### 5. Configure paths:

Update any paths in your code or `.env` if necessary to point to the `processed/` folders for your analysis.
