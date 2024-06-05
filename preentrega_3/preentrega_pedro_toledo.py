# %%
import requests
import numpy as np
import pandas as pd
import datetime
import sqlalchemy


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
import config
user = config.user
passw = config.passw
host = config.host
port = config.port
database = config.database
schema = config.schema

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

insertar_sin_repeticion(conn, engine, schema, 'chuck_jokes2')