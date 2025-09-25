from fastapi import FastAPI
from pydantic import BaseModel
import mlflow.pyfunc

app = FastAPI()

class CarFeatures(BaseModel):
    Year: int
    Present_Price: float
    Kms_Driven: int

# Загружаем модель из MLflow
model = mlflow.pyfunc.load_model("models:/car-price/1")

@app.post("/predict")
def predict(features: CarFeatures):
    data = [[features.Year, features.Present_Price, features.Kms_Driven]]
    prediction = model.predict(data)
    return {"predicted_price": float(prediction[0])}
