import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pathlib import Path

# ── App setup ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Customer Prediction API",
    description="Loads a pre-trained model and returns predictions from customer financial features.",
    version="1.0.0",
)

# ── Model loading ─────────────────────────────────────────────────────────────
MODEL_PATH = Path("artifacts/model.joblib")

def load_model():
    if not MODEL_PATH.exists():
        raise RuntimeError(f"Model file not found at '{MODEL_PATH}'. "
                           "Make sure model.joblib is placed in the artifacts/ folder.")
    return joblib.load(MODEL_PATH)

try:
    model = load_model()
    print(f"✅ Model loaded successfully from {MODEL_PATH}")
except RuntimeError as e:
    print(f"⚠️  Warning: {e}")
    model = None


# ── Input / Output schemas ────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    customer_id: str = Field(..., example="CUST001")
    num_transactions: int = Field(..., example=15)
    total_debit: float = Field(..., example=-1250.50)
    total_credit: float = Field(..., example=3500.00)
    avg_amount: float = Field(..., example=150.03)
    has_rent: int = Field(..., ge=0, le=1, example=1)
    has_salary: int = Field(..., ge=0, le=1, example=1)


class PredictResponse(BaseModel):
    customer_id: str
    prediction: int
    prediction_label: str
    probability: float | None = None


# Features fed into the model (must match training order)
FEATURE_COLUMNS = [
    "num_transactions",
    "total_debit",
    "total_credit",
    "avg_amount",
    "has_rent",
    "has_salary",
]

LABEL_MAP = {0: "low_risk", 1: "high_risk"}  # adjust to match your training labels


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
def root():
    """Health-check endpoint."""
    return {"status": "ok", "model_loaded": model is not None}


@app.post("/predict", response_model=PredictResponse, tags=["Prediction"])
def predict(payload: PredictRequest):
    """
    Accept customer financial features and return a model prediction.
    """
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Model is not loaded. Place model.joblib in the artifacts/ folder and restart.",
        )

    # Build feature vector in the exact order the model was trained on
    features = np.array([[
        payload.num_transactions,
        payload.total_debit,
        payload.total_credit,
        payload.avg_amount,
        payload.has_rent,
        payload.has_salary,
    ]])

    prediction = int(model.predict(features)[0])

    # Return probability if the model supports it (e.g. RandomForest, LogisticRegression)
    probability = None
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(features)[0]
        probability = round(float(proba[prediction]), 4)

    return PredictResponse(
        customer_id=payload.customer_id,
        prediction=prediction,
        prediction_label=LABEL_MAP.get(prediction, str(prediction)),
        probability=probability,
    )