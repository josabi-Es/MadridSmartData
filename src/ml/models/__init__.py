"""Model registry -- the only place to touch when adding a model.

Adding one:
  1. cp xgboost.py <nuevo>.py, adjust PARAMS / fit / predict / name
  2. add a line below

Commenting a line out disables that model; nothing else in src/ml/ knows any
model by name.
"""

from src.ml.models.sarimax import Sarimax
from src.ml.models.seasonal_naive import SeasonalNaive
from src.ml.models.xgboost import XGBoost

REGISTRY = {
    "seasonal_naive": SeasonalNaive,
    "xgboost": XGBoost,
    "sarimax": Sarimax,
}
