from fastapi import FastAPI

from server.models.predict import PredictInput, PredictOutput
from server.services.predict_service import PredictService

from datetime import date

app = FastAPI()


@app.get('/last_trained_date')
def get_last_trained_date() -> date:
    predict_service = PredictService()
    return predict_service.get_last_trained_data()


@app.post('/predict')
def post_predict(body: PredictInput) -> list[PredictOutput]:
    predict_service = PredictService()
    return predict_service.predict(body)
