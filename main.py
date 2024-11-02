import os
from nbconvert import NotebookExporter
from nbformat import read, NO_CONVERT
from subprocess import run

def run_notebook(notebook_path):
    """Runs a Jupyter notebook."""
    print(f"Running notebook: {notebook_path}")
    # Use nbconvert to execute the notebook
    run(["jupyter", "nbconvert", "--to", "notebook", "--execute",
         "--inplace", notebook_path])

def main():
    # List of your notebook paths
    notebooks = [
        "ETL/rapido_furiosos/dimensiones/dm_cliente.ipynb",
        "ETL/rapido_furiosos/dimensiones/dm_fecha.ipynb",
        "ETL/rapido_furiosos/dimensiones/dm_sede.ipynb",
        "ETL/rapido_furiosos/dimensiones/dm_tiempo.ipynb",
        "ETL/rapido_furiosos/dimensiones/dm_mensajero.ipynb",
        "ETL/rapido_furiosos/hechos/hecho_fact_mensajeria_cliente_diaria.ipynb",
       # "ETL/rapido_furiosos/hechos/hecho_mensajeria_cliente_hora.ipynb",
        "ETL/rapido_furiosos/hechos/hecho_mensajeria_cliente_snapshot.ipynb",
       # "ETL/rapido_furiosos/hechos/hecho_novedad.ipynb"

    ]

    # Run each notebook
    for notebook in notebooks:
        if os.path.exists(notebook):
            run_notebook(notebook)
        else:
            print(f"Notebook {notebook} not found.")

if __name__ == "__main__":
    main()




