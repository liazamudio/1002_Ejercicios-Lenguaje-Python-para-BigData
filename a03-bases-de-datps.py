# Realizamos las importaciones
import mysql.connector
import pandas as pd
import numpy as np

# Conectamos con la base de datos
cnx = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password='2023*DaSci',
    database='titanic'
)

# Generamos una tupla de numeros aleatorios
random_numbers = np.random.randint(0, 101, 10)                  # Generamos los números
valores_int = random_numbers.astype(int)                        # Los convertimos en valores int
random_values = tuple(valores_int)                              # Convertimos el arreglo en una tupla
print(f"\nNúmeros aleatorios en una tupla: {random_values}\n")  # Comprobamos la tupla

# Creamos el cursor
cursor = cnx.cursor()

# Consulta SQL y obtención de resultados de la misma
sql_query = "SELECT * FROM titanic.train"
cursor.execute(sql_query)                                       # Ejecutamos la consulta
pass_survived = cursor.fetchall()                               # Obtención de los resultados

# Crear un DataFrame y lo filtramos
df_pass_surv = pd.DataFrame(pass_survived, columns=[col[0] for col in cursor.description])  # Convertimos el resultado en un dataframe
df_surv_filt = df_pass_surv[df_pass_surv['idPassenger'].isin(random_values)]                # Filtramos en dataframe
# print(df_surv_filt)                                                                       # Lo revisamos

# CREACIÓN DE LAS TABLAS CON LOS DATOS FILTRADOS

# Creamos la tabla pass_surv_filt
sql_create_table_pass_surv_filt = """
CREATE TABLE IF NOT EXISTS pass_surv_filt (
    idPassenger INT,
    Survived INT,
    Name TEXT,
    Sex TEXT,
    Ticket TEXT
);
"""
cursor.execute(sql_create_table_pass_surv_filt)

# Creamos la tabla pass_cabin_filt
sql_create_table_pass_cabin_filt = """
CREATE TABLE IF NOT EXISTS pass_cabin_filt (
    idPassenger INT,
    Name TEXT,
    Sex TEXT,
    Ticket TEXT,
    Fare DOUBLE,
    Cabin TEXT
);
"""
cursor.execute(sql_create_table_pass_cabin_filt)

# INSERTAMOS LOS DATOS EN LAS TABLAS

for index, row in df_surv_filt.iterrows():
    cursor.execute("INSERT INTO pass_surv_filt (idPassenger, Survived, Name, Sex, Ticket) VALUES (%s, %s, %s, %s, %s)", (row.idPassenger, row.Survived, row.Name, row.Sex, row.Ticket))
    cursor.execute("INSERT INTO pass_cabin_filt (idPassenger, Name, Sex, Ticket, Fare, Cabin) VALUES (%s, %s, %s, %s, %s, %s)", (row.idPassenger, row.Name, row.Sex, row.Ticket, row.Fare, row.Cabin))

# Cerramos
cnx.commit()                    # Confirmamos los cambios
cursor.close()                  # Cerramos cursor
cnx.close()                     # Cerramos la conexión