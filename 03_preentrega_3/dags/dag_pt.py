# %%
import requests
import numpy as np
import pandas as pd
import datetime
import sqlalchemy


from airflow import DAG
from airflow.macros import ds_add
from airflow.operators.python_operator import PythonOperator

# %%
'''
Vamos a utilizar la Api de chuck norris para obtener chistes aleatorios.

https://api.chucknorris.io/

'''

# %%
#Definimos la funcion para llamar la api y obtener un chiste aleatorio

def get_joke():
    url = 'https://api.chucknorris.io/jokes/random'
    response = requests.get(url)
    data = response.json()
    data = pd.DataFrame([data])

    data['fecha_insercion'] = datetime.datetime.now()
    data = data[['id', 'created_at', 'url', 'value', 'fecha_insercion']]
    return data


# %%
import os
import dotenv
dotenv.load_dotenv('config.env')

user = os.environ.get('USER')
passw = os.environ.get('PASSW')
host = os.environ.get('HOST')
port = os.environ.get('PORT')
database = os.environ.get('DATABASE')
schema = os.environ.get('SCHEMA')

# %%
#coneccion a redshift

def conexion_redshift(user, passw, host, port, database):
    conn_string = f'postgresql://{user}:{passw}@{host}:{port}/{database}?sslmode=require'
    engine = sqlalchemy.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine

conn, engine = conexion_redshift(user, passw, host, port, database)

# %%
# creamos la tabla en redshift, puede ser a traves de un query o con pandas

def create_table_query(conn, engine, schema, table_name):
    query = f'''
    CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
    id VARCHAR(50) PRIMARY KEY,
    created_at TIMESTAMP,
    url VARCHAR(500),
    value VARCHAR(1000),
    fecha_insercion TIMESTAMP
    )
    '''
    conn.execute(query)
    return


create_table_query(conn, engine, schema, 'chuck_jokes2')

# %%
#Insert de un registro
# get_joke().to_sql(
#     name='chuck_jokes2',
#     con=engine,
#     schema=schema,
#     if_exists='append',
#     index=False,
#     dtype={ 'id': sqlalchemy.types.VARCHAR(50),
#             'created_at': sqlalchemy.types.TIMESTAMP,
#             'url': sqlalchemy.types.VARCHAR(500),
#             'value': sqlalchemy.types.VARCHAR(1000),
#             'fecha_insercion': sqlalchemy.types.TIMESTAMP
#             }
# )

# %%
#Insertar un registro sin repetición de id

def insertar_sin_repeticion(conn, engine, schema, table_name):

    #Obtener registro de id de chistes
    query = f'SELECT id FROM {schema}.{table_name}'

    #Obtener ids de chistes en la tabla
    ids_en_tabla = pd.read_sql(query, conn)['id'].values

    #Nos aseguramos que el id no este en la tabla, si está, obtenemos otro chiste.
    while True:
        data = get_joke()
        joke_id = data['id'][0]

        if joke_id not in ids_en_tabla:
            break
        else:
            print('Chiste repetido, obteniendo otro chiste')
    
    data.to_sql(
            name='chuck_jokes2',
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            dtype={ 'id': sqlalchemy.types.VARCHAR(50),
                    'created_at': sqlalchemy.types.TIMESTAMP,
                    'url': sqlalchemy.types.VARCHAR(500),
                    'value': sqlalchemy.types.VARCHAR(1000),
                    'fecha_insercion': sqlalchemy.types.TIMESTAMP
                    }
    )

    return





default_args = {
    'owner': 'pedro-toledo',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 6, 10),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

with DAG('agrega_chiste_chuck_norris',
          description='DAG que hace un request a una API y guarda un chiste en una tabla de redshift',
          schedule_interval='0 12 * * *',
          catchup=False,
          default_args=default_args) as dag:
    
    kwargs1 = {'conn': conn, 'engine': engine, 'schema': schema, 'table_name': 'chuck_jokes2'}
    task1 = PythonOperator(task_id='create_table_query', 
                           python_callable=create_table_query, 
                           op_kwargs=kwargs1, 
                           dag=dag)

    kwargs2 = {'conn': conn, 'engine': engine, 'schema': schema, 'table_name': 'chuck_jokes2'}
    task2 = PythonOperator(task_id='insert_data',
                            python_callable=insertar_sin_repeticion, 
                            op_kwargs=kwargs2, 
                            dag=dag)

    task1 >> task2