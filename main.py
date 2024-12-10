import os
import asyncio
import warnings
import yaml
from subprocess import run
from sqlalchemy import create_engine, text, inspect
import platform
import psycopg2

# Establecer la política de bucle de eventos para Windows solamente
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

warnings.filterwarnings("ignore", category=RuntimeWarning)

# Leer configuración de la conexión desde el archivo YAML
with open('./configBD/config.yml', 'r') as f:
    config = yaml.safe_load(f)
    config_rp = config['rapidofuriosos']
    config_etl = config['bodega']

# Crear URLs de conexión a las bases de datos
url_rp = (f"{config_rp['driver']}://{config_rp['user']}:{config_rp['password']}@{config_rp['host']}:"
          f"{config_rp['port']}/{config_rp['db']}")
url_etl = (f"{config_etl['driver']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['db']}")

# Crear el motor de conexión a la base de datos ETL
etl_conn = create_engine(url_etl)

def run_notebook(notebook_path):
    """Ejecuta un notebook de Jupyter."""
    print(f"Running notebook: {notebook_path}")
    run(["jupyter", "nbconvert", "--to", "notebook", "--execute",
         "--inplace", "--clear-output", notebook_path])

def check_data_changes():
    """Verifica si hay cambios en los datos (dimensiones y hechos)."""
    # Implementa la lógica de verificación de cambios aquí.
    # Ejemplo de retorno:
    return True  # Cambia esto según tu verificación

def check_if_db_deleted():
    """Verifica si la base de datos (dimensiones y hechos) ha sido eliminada."""
    # Implementa la lógica de verificación de eliminación de la base de datos aquí.
    # Ejemplo de retorno:
    return False  # Cambia esto según tu verificación

def main():


    # Ruta a los notebooks que se deben ejecutar
    notebooks = [
        os.path.join("rapido_furiosos", "dimensiones", "dm_cliente.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_fecha.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_sede.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_tiempo.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_mensajero.ipynb"),
        os.path.join("rapido_furiosos", "hechos", "hecho_fact_mensajeria_cliente_diaria.ipynb"),
        os.path.join("rapido_furiosos", "hechos", "hecho_mensajeria_cliente_snapshot.ipynb"),
        os.path.join("rapido_furiosos", "hechos", "hecho_novedad.ipynb"),
        os.path.join("rapido_furiosos", "hechos", "hecho_novedad_clustering.ipynb")
    ]

    # Verificar si hay cambios en los datos o si la base de datos ha sido eliminada
    data_changed = check_data_changes()
    db_deleted = check_if_db_deleted()

    if not data_changed and not db_deleted:
        print("Las dimensiones y hechos ya están creados y sus registros no tienen cambios.")
        return

    if db_deleted:
        print("Las bases de datos de dimensiones y hechos han sido eliminadas, se ejecutarán los notebooks.")
    elif data_changed:
        print("Se detectaron cambios en los registros, se ejecutarán los notebooks.")

    # Ejecutar cada notebook
    for notebook in notebooks:
        print(f"Checking notebook: {notebook}")
        if os.path.exists(notebook):
            run_notebook(notebook)
        else:
            print(f"Notebook {notebook} not found.")
    conn = psycopg2.connect(dbname=config_etl['db'], user=config_etl['user'], password=config_etl['password'],
                                host=config_etl['host'], port=config_etl['port'])
    cur = conn.cursor()
    with open('sqlscripts.yml', 'r') as f:
        sql = yaml.safe_load(f)
        for key, val in sql.items():
            cur.execute(val)
            conn.commit()
if __name__ == "__main__":
    main()
