FROM python:3.11

WORKDIR /app

RUN pip install apache-airflow[providers-postgres]

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/airflow/dags
COPY dag_pt.py .

EXPOSE 8080

CMD ["airflow", "scheduler"]
CMD ["airflow", "webserver"]

