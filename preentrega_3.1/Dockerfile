FROM python:3.11

WORKDIR ./app

RUN pip install apache-airflow[providers-postgres]

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


WORKDIR ./dags
COPY dag_pedro_toledo.py ./dags
COPY config.env ./dags


EXPOSE 8080

CMD ["python", "-u", "dag_pedro_toledo.py"]