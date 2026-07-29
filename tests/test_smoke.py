import numpy as np
import pandas as pd

from src.ml.core.features import build
from src.ml.models import REGISTRY

ULTIMO_REAL = pd.Timestamp("2024-06-30")
HORIZONTE = 30


def _historico() -> pd.DataFrame:
    """Serie diaria con ciclo anual + semanal, como un gas de verdad."""
    fechas = pd.date_range("2023-01-01", ULTIMO_REAL, freq="D")
    i = np.arange(len(fechas))
    dato = 30 + 10 * np.sin(2 * np.pi * i / 365) + 3 * (fechas.dayofweek < 5)
    return pd.DataFrame(
        {"estacion": 1, "cod_dis": "1", "fecha": fechas, "dato": dato}
    )


def test_registry_expone_los_modelos():
    assert set(REGISTRY) == {"seasonal_naive", "xgboost", "sarimax"}


def test_el_futuro_empieza_tras_el_ultimo_dato_real():
    """El fallo que arruinaría todo en silencio: predecir sobre datos ya vistos."""
    train, futuro = build(_historico(), HORIZONTE)

    assert train["fecha"].max() == ULTIMO_REAL
    assert futuro["fecha"].min() == ULTIMO_REAL + pd.Timedelta(days=1)
    assert futuro["fecha"].max() == ULTIMO_REAL + pd.Timedelta(days=HORIZONTE)


def test_cada_modelo_predice_un_valor_por_fila():
    train, futuro = build(_historico(), HORIZONTE)

    for nombre, cls in REGISTRY.items():
        modelo = cls()
        modelo.fit(train)
        pred = modelo.predict(futuro)
        assert len(pred) == len(futuro), nombre
        assert np.isfinite(pred).all(), nombre
