import numpy as np
import pandas as pd

from src.ml.models.base import ForecastModel
from src.ml.models.sklearn_models import (
    DecisionTreeModel,
    MLPModel,
    NaiveModel,
    RandomForestModel,
    XGBoostModel,
)

ALL_MODELS = [NaiveModel, DecisionTreeModel, RandomForestModel, XGBoostModel, MLPModel]


def _toy_data():
    x = pd.DataFrame({"lag_1": np.arange(20.0), "dow": [i % 7 for i in range(20)]})
    y = pd.Series(np.arange(20.0) + 1)
    return x, y


def test_all_models_are_forecast_models():
    for cls in ALL_MODELS:
        assert issubclass(cls, ForecastModel)


def test_all_models_fit_predict_return_right_shape():
    x, y = _toy_data()
    x_train, x_test = x.iloc[:15], x.iloc[15:]
    y_train = y.iloc[:15]

    for cls in ALL_MODELS:
        model = cls()
        model.fit(x_train, y_train)
        preds = model.predict(x_test)
        assert len(preds) == len(x_test)


def test_all_models_have_a_name():
    for cls in ALL_MODELS:
        assert isinstance(cls().name, str)
        assert cls().name


def test_naive_model_predicts_lag_1_unchanged():
    x, y = _toy_data()
    model = NaiveModel()
    model.fit(x, y)

    preds = model.predict(x)

    assert list(preds) == list(x["lag_1"])
