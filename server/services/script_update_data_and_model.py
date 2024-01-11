import os
from dotenv import load_dotenv

import pandas as pd
import psycopg2

from statsforecast import StatsForecast
from statsforecast.models import Naive, MSTL
import joblib

load_dotenv(override=True)


def run_script():
    class DbRepository:
        conenction = None
        cursor = None

        def __init__(self):
            db_params = {
                'host': os.environ['DB_HOST'],
                'database': os.environ['DB_DATABASE'],
                'user': os.environ['DB_USER'],
                'password': os.environ['DB_PASS'],
                'port': os.environ['DB_PORT']
            }
            self.connection = psycopg2.connect(**db_params)
            self.cursor = self.connection.cursor()

        def execute_select_query(self, query):
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results

        def execute_insert_query(self, query):
            self.cursor.execute(query)
            self.connection.commit()

        def close_connection(self):
            self.connection.close()

    def update_raw_data(db_repository):
        html_response = pd.read_html('http://www.ipeadata.gov.br/ExibeSerie.aspx?module=m&serid=1650971490&oper=view')
        df = html_response[2]
        df = df.drop(0)
        df.columns = ['date', 'value']

        query = """
            SELECT
                date
            FROM
                tb_price_raw_data;
        """
        results = db_repository.execute_select_query(query)
        dates = [result[0] for result in results]

        df = df[~df.date.isin(dates)]

        if df.empty:
            print('Nao ha atualizacoes')
            return df

        def create_query_insert(date, value):
            query = f"""
                INSERT
                    INTO
                    tb_price_raw_data ( 
                    date,
                    value 
                    )
                VALUES (
                    '{date}',
                    '{value}'
                    );
            """
            return query

        df['query_insert'] = df.apply(lambda r: create_query_insert(r['date'], r['value']), axis=1)
        insert_query = '\n'.join(df['query_insert'].to_list())
        db_repository.execute_insert_query(insert_query)
        return df

    def update_formated_data(db_repository, df):
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
        df['value'] = df['value'].str.replace(',', '.').astype(float)
        df['value'] = df['value'] / 100

        df = df.sort_values('date', ascending=True)

        def create_query_insert(date, value):
            query = f"""
                INSERT
                    INTO
                    tb_price ( 
                    date,
                    value 
                    )
                VALUES (
                    '{date}',
                    {value}
                    );
            """
            return query

        df['query_insert'] = df.apply(lambda r: create_query_insert(r['date'], r['value']), axis=1)
        insert_query = '\n'.join(df['query_insert'].to_list())
        db_repository.execute_insert_query(insert_query)

    def create_new_model(db_repository):
        query = """
            SELECT
                date,
                value
            FROM
                tb_price;
        """
        results = db_repository.execute_select_query(query)
        df = pd.DataFrame(results, columns=['ds', 'y'])
        df['unique_id'] = 0

        model = StatsForecast(models=[MSTL(season_length=[247, 22, 5], trend_forecaster=Naive())], freq='B', n_jobs=-1)
        model.fit(df)
        joblib.dump(model, 'model.joblib')

    db_repository = DbRepository()
    df = update_raw_data(db_repository)
    if df.empty:
        return {
            "status": "Não há novas atualizações."
        }
    update_formated_data(db_repository, df)
    create_new_model(db_repository)
    return {
        "status": "Dados e Modelo atualizados com sucesso!"
    }
