from fastapi import FastAPI
from pydantic import BaseModel
import mlflow.sklearn

app = FastAPI(title="a2k_procar API")

class CarFeatures(BaseModel):
    Year: int
    Present_Price: float
    Kms_Driven: int

model = mlflow.sklearn.load_model("model")

@app.post("/predict")
def predict(features: CarFeatures):
    data = [[features.Year, features.Present_Price, features.Kms_Driven]]
    prediction = model.predict(data)
    return {"predicted_price": float(prediction[0])}
