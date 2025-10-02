"""
FastAPI приложение для предсказания цены автомобиля (procar)
"""

import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from loguru import logger

from src.inference import load_model, predict


logger.info("Загружаем модель...")
MODEL_PATH = os.path.join("models", "car_price_model.joblib")
MODEL = load_model(MODEL_PATH)
logger.info("Модель успешно загружена")


app = FastAPI(title="ProCar Model API", version="1.0")


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
    logger.info(f"Получены данные: {features}")
    try:
        data = pd.DataFrame([features.model_dump()])
        prediction = predict(MODEL, data)
        price = float(prediction[0])
    except Exception as e:
        logger.error(f"Ошибка предсказания: {e}")
        raise HTTPException(
            status_code=500,
            detail="Ошибка при расчете предсказания"
        )
    
    return {"prediction": price}


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