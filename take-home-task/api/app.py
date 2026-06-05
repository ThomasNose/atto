from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib

app = FastAPI()

model = joblib.load("artifacts/model.joblib")


class CustomerFeatures(BaseModel):
    transaction_count: int
    total_debit: float
    total_credit: float
    avg_amount: float
    rent: int = 0
    netflix: int = 0
    tesco: int = 0
    payroll: int = 0
    bonus: int = 0


@app.post("/predict")
def predict(payload: CustomerFeatures):
    if model is None:
        raise HTTPException(status_code=500, detail="Model not loaded")

    X = [[
        payload.transaction_count,
        payload.total_debit,
        payload.total_credit,
        payload.avg_amount,
        payload.rent,
        payload.tesco,
        payload.netflix,
        payload.payroll,
        payload.bonus,
    ]]

    proba = model.predict_proba(X)[0][1]
    pred = int(proba >= 0.5)

    return {
        "probability": float(proba),
        "prediction": pred
    }