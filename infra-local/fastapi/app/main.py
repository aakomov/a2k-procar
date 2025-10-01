# app/main.py
from fastapi import FastAPI
from pydantic import BaseModel
import mlflow.pyfunc
import os
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import mlflow

mlflow.set_tracking_uri("http://localhost:5000")

# MODEL_URI = os.environ.get("MODEL_URI", "models:/car_price_model/Production")
MODEL_URI = os.environ.get("MODEL_URI", "models:/car_price_model@champion")
model = mlflow.pyfunc.load_model(MODEL_URI)

app = FastAPI(title="Car Price Predictor")

REQUESTS = Counter('requests_total', 'Total HTTP requests')
INFER_TIME = Histogram('inference_seconds', 'Inference time')

class CarInput(BaseModel):
    Car_Name: str
    Year: int
    Present_Price: float
    Kms_Driven: int
    Fuel_Type: str
    Seller_Type: str
    Transmission: str
    Owner: int

@app.post("/predict")
def predict(data: CarInput):
    REQUESTS.inc()
    import time
    start = time.time()
    df = [{
        "Car_Name": data.Car_Name,
        "Year": data.Year,
        "Present_Price": data.Present_Price,
        "Kms_Driven": data.Kms_Driven,
        "Fuel_Type": data.Fuel_Type,
        "Seller_Type": data.Seller_Type,
        "Transmission": data.Transmission,
        "Owner": data.Owner
    }]
    pred = model.predict(df)
    INFER_TIME.observe(time.time() - start)
    return {"prediction": float(pred[0])}

@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)