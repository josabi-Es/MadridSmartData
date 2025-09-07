import geopandas as gpd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Paths from .env
DISTRITOS_PATH = os.getenv("DISTRITOS_PATH", "data/processed/districts/Distritos_geo.json")
DISTRITOS_FILTERED_PATH = os.getenv("DISTRITOS_FILTERED_PATH", "data/processed/districts/Distritos_filtrados.geojson")

# Read the original file
gdf = gpd.read_file(DISTRITOS_PATH)

# Select the columns of interest
gdf_selected = gdf[["COD_DIS", "NOMBRE", "geometry"]]

# Remove duplicate rows
gdf_unique = gdf_selected.drop_duplicates()

# Save the preprocessed GeoDataFrame
gdf_unique.to_file(DISTRITOS_FILTERED_PATH, driver="GeoJSON")

print(f"File saved at: {DISTRITOS_FILTERED_PATH}")
print(gdf_unique.head(2))
