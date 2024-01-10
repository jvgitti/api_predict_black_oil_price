import joblib
import json
from numpy import busday_count


class PredictService:
    def predict(self, body):
        date = body.date
        model = joblib.load('model.joblib')
        last_date = model.last_dates[0].date()
        h = busday_count(last_date, date)

        df = model.predict(h=h, level=[90])
        df = df[['ds', 'MSTL']]
        df.columns = ['date', 'value']
        response = json.loads(df.to_json(orient='records'))
        return response

    def get_last_trained_data(self):
        model = joblib.load('model.joblib')
        return model.last_dates[0].date()
