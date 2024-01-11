from pydantic import BaseModel
from datetime import date


class PredictInput(BaseModel):
    date: date


class PredictOutput(BaseModel):
    date: date
    value: float


class UpdateDataAndModelOutput(BaseModel):
    status: str
