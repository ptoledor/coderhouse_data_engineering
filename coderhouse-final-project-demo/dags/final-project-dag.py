"""
Run first:
create table stocks_data (Open float,
High float,
Low float,
Close float,
Volume float,
Dividends float,
Stock_splits float,
Stock varchar,
Date timestamp);
"""

import pandas as pd
import os
import json
import logging
import psycopg2

import yfinance as yf

from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

default_args = {
    'owner': 'axelfurlan',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

WAREHOUSE_HOST     = os.environ.get('WAREHOUSE_HOST')
WAREHOUSE_DBNAME   = os.environ.get('WAREHOUSE_DBNAME')
WAREHOUSE_USER     = os.environ.get('WAREHOUSE_USER')
WAREHOUSE_PASSWORD = os.environ.get('WAREHOUSE_PASSWORD')


def retrieve_data_from_api(**context):
    df_final = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock_splits'])
    with open('dags/config.json', 'r') as json_config:
        config = json.load(json_config)

    for stock in config['stocks']:
        logging.info(f"Retrieving info for {stock}")
        stock_data = yf.Ticker(stock)
        df = stock_data.history(period="1d")
        df = df.reset_index(level=0)
        df['Stock'] = stock.upper()
        df.rename(columns={"Stock Splits": "Stock_splits"}, inplace=True)
        df_final = df_final.append(df)

    csv_filename = f"{context['ds']}_stocks_data.csv"
    df_final.columns = df_final.columns.str.lower()
    df_final.to_csv(csv_filename, index=False)

    return csv_filename


def save_data_to_dw(**context):
    csv_filename = context['ti'].xcom_pull(task_ids='get_data')
    df = pd.read_csv(csv_filename)

    conn_string = f"host='{WAREHOUSE_HOST}' dbname='{WAREHOUSE_DBNAME}' user='{WAREHOUSE_USER}' password='{WAREHOUSE_PASSWORD}'"
    conn = psycopg2.connect(conn_string)
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM stocks_data WHERE date = '{ds_add(context['ds'], 1)}';")
    conn.close()

    engine = create_engine(f'postgresql://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}@{WAREHOUSE_HOST}:5432/{WAREHOUSE_DBNAME}')
    df.to_sql('stocks_data', engine, if_exists="append", index=False)


def verify_threshold_and_verify(**context):
    csv_filename = context['ti'].xcom_pull(task_ids='get_data')
    df = pd.read_csv(csv_filename)

    with open('dags/config.json', 'r') as json_config:
        config = json.load(json_config)

    for stock in config["stocks"]:
        min_t = config["thresholds"][stock].get("min")
        max_t = config["thresholds"][stock].get("max")

        current_date = ds_add(context['ds'], 1)
        close_value = df.loc[(df.stock == stock) & (df.date == current_date), 'close'].values[0]
        print(f"Close value for stock {stock} is {close_value}. thresholds are: between {min_t} and {max_t}")
        if min_t > close_value or max_t < close_value:

            if min_t > close_value:
                subject = f"Stock {stock} is under the threshold"
            else:
                subject = f"Stock {stock} is over the threshold"

            body = f"""
                Close value for stock {stock} is {close_value}.
                Thresholds values are: between {min_t} and {max_t}
            """

            message = Mail(
                from_email=os.environ['EMAIL_FROM'],
                to_emails=os.environ['EMAIL_TO'],
                subject=subject,
                html_content=body)

            sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
            response = sg.send(message)
            print(response.status_code)


with DAG('get_stocks_data_and_alert',
          description='DAG that retrieves data from API and saves it into a table in a Data Warehouse',
          schedule_interval='0 12 * * *',
          catchup=False,
          default_args=default_args) as dag:

    get_data = PythonOperator(task_id='get_data', python_callable=retrieve_data_from_api, dag=dag, provide_context=True)

    save_data = PythonOperator(task_id='save_data', python_callable=save_data_to_dw, dag=dag, provide_context=True)

    send_email_if_anomaly = PythonOperator(task_id='send_email_if_anomaly', python_callable=verify_threshold_and_verify, dag=dag, provide_context=True)

    get_data >> save_data
    get_data >> send_email_if_anomaly
