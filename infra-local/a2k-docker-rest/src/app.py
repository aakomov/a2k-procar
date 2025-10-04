"""
FastAPI приложение для предсказания цены автомобиля (procar) с метриками Prometheus
"""

import os
import time
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from loguru import logger
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from src.inference import load_model, predict


# === Загружаем модель ===
logger.info("Загружаем модель...")
MODEL_PATH = os.path.join("models", "car_price_model.joblib")
MODEL = load_model(MODEL_PATH)
logger.info("Модель успешно загружена")

# === Инициализация FastAPI ===
app = FastAPI(title="ProCar Model API", version="1.0")

# === Метрики Prometheus ===
PREDICTION_REQUESTS = Counter(
    "procar_prediction_total",
    "Количество запросов на предсказание"
)
PREDICTION_ERRORS = Counter(
    "procar_prediction_errors_total",
    "Количество ошибок предсказаний"
)
PREDICTION_LATENCY = Histogram(
    "procar_prediction_latency_seconds",
    "Время обработки запроса предсказания (секунды)"
)


class CarFeatures(BaseModel):
    """Характеристики автомобиля для предсказания цены"""
    Year: int
    Present_Price: float
    Kms_Driven: int
    Owner: int
    Car_Age: int
    Kms_Per_Year: float
    Fuel_Type: str
    Seller_Type: str
    Transmission: str


@app.get("/")
def health_check() -> dict:
    """Health check"""
    return {"status": "ok", "model": "procar"}


@app.post("/predict")
def make_prediction(features: CarFeatures) -> dict:
    """Сделать предсказание цены автомобиля"""
    PREDICTION_REQUESTS.inc()
    start_time = time.time()
    logger.info(f"Получены данные: {features}")
    try:
        data = pd.DataFrame([features.model_dump()])
        prediction = predict(MODEL, data)
        price = float(prediction[0])
    except Exception as e:
        PREDICTION_ERRORS.inc()
        logger.error(f"Ошибка предсказания: {e}")
        raise HTTPException(
            status_code=500,
            detail="Ошибка при расчете предсказания"
        )
    finally:
        PREDICTION_LATENCY.observe(time.time() - start_time)

    return {"prediction": price}


@app.get("/metrics")
def metrics() -> Response:
    """Метрики Prometheus"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# {
#   "Year": 2015,
#   "Present_Price": 9.85,
#   "Kms_Driven": 69000,
#   "Owner": 0,
#   "Car_Age": 8,
#   "Kms_Per_Year": 9875.0,
#   "Fuel_Type": "Petrol",
#   "Seller_Type": "Dealer",
#   "Transmission": "Manual"
# }