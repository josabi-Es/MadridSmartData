# Preprocessing Scripts - Quick Guide

## Overview
This repository contains preprocessing scripts for Madrid open data, including air quality, traffic, and district data. The scripts are designed to work with the provided raw datasets in Parquet format.

## Scripts
1. `District_Preprocessing.py` – Preprocess district GeoJSON data.  
2. `QualityAir_Station_Preprocessing.py` – Preprocess air quality and stations data.  
3. `Traffic_Preprocessing.py` – Preprocess traffic data.  

## Setup
1. Copy the `.env.template` to a `.env` file wherever you prefer and update paths if necessary.  
   ```bash
   cp .env.template .env
   ```

## ⚠️ Warning - Data Requirements

**Important:** Ensure that the raw data you place in the input folders matches the **Madrid Open Data** structure and metadata.  
If the data differs in format or schema, the preprocessing scripts may fail.

## Running the scripts

Execute each script using Python’s -m option:
```
python -m District_Preprocessing
python -m QualityAir_Station_Preprocessing
python -m Traffic_Preprocessing
```
## Data Flow & Folder Structure

                            ┌────────────────────────────┐
                            │       Raw Data Paths       │
                            │----------------------------│
                            │ RAW_CALIDAD_AIRE_PATH      │
                            │ STATIONS_RAW_PATH          │
                            │ TRAFFIC_RAW_PATH           │
                            │ POSITION_RAW_PATH          │
                            │ DISTRITOS_PATH             │
                            └────────────────────────────┘
                                           │
                             ┌──────────────────────────────┐
                             │    Preprocessing & Cleaning  │
                             └──────────────────────────────┘
                                           │
                             ┌────────────────────────────┐
                             │       Processed Data       │
                             │----------------------------│
                             │ POLLUTION_PROCESSED_PATH   │
                             │ TRAFFIC_PROCESSED_PATH     │
                             │ DISTRITOS_FILTERED_PATH    │
                             └────────────────────────────┘