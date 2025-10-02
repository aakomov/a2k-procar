"""
Module: inference.py
Description: 
    Load the trained model and make predictions on new car data.
"""

import joblib
import pandas as pd
from typing import Union, List
from sklearn.base import BaseEstimator


def save_model(model: BaseEstimator, model_path: str) -> None:
    """Save trained model to disk."""
    joblib.dump(model, model_path)


def load_model(model_path: str) -> BaseEstimator:
    """Load model from disk."""
    try:
        model = joblib.load(model_path)
        return model
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Model not found at {model_path}") from e


def predict(model: BaseEstimator, data: Union[pd.DataFrame, dict]) -> List[float]:
    """
    Predict car prices for new data.

    Parameters
    ----------
    model : BaseEstimator
        The trained model.
    data : pd.DataFrame or dict
        New input data for prediction.

    Returns
    -------
    List[float]
        Predictions of car prices.
    """
    if isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        df = data

    preds = model.predict(df)
    return preds.tolist()
