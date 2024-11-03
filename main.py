import os
import asyncio
import warnings
from subprocess import run

# Establecer la política de bucle de eventos para Windows al inicio
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
warnings.filterwarnings("ignore", category=RuntimeWarning)

def run_notebook(notebook_path):
    """Runs a Jupyter notebook."""
    print(f"Running notebook: {notebook_path}")
    run(["jupyter", "nbconvert", "--to", "notebook", "--execute",
         "--inplace", "--clear-output", notebook_path])

def check_data_changes():
    """Placeholder for data change check logic."""
    # Implement your logic to check for changes in dimensions and facts
    # Return True if there are changes, False otherwise
    # Example:
    return True  # Replace with actual check

def check_if_db_deleted():
    """Placeholder for checking if DB dimensions and facts are deleted."""
    # Implement your logic to check if the database has been deleted
    # Return True if deleted, False otherwise
    # Example:
    return False  # Replace with actual check

def main():
    notebooks = [
        os.path.join("rapido_furiosos", "dimensiones", "dm_cliente.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_fecha.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_sede.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_tiempo.ipynb"),
        os.path.join("rapido_furiosos", "dimensiones", "dm_mensajero.ipynb"),
        os.path.join("rapido_furiosos", "hechos", "hecho_fact_mensajeria_cliente_diaria.ipynb"),
        os.path.join("rapido_furiosos", "hechos", "hecho_mensajeria_cliente_snapshot.ipynb"),
        os.path.join("rapido_furiosos", "hechos", "hecho_novedad.ipynb")
    ]

    data_changed = check_data_changes()
    db_deleted = check_if_db_deleted()

    if not data_changed and not db_deleted:
        print("Las dimensiones y hechos ya están creados y sus registros no tienen cambios.")
        return

    if db_deleted:
        print("Las bases de datos de dimensiones y hechos han sido eliminadas, se ejecutarán los notebooks.")
    elif data_changed:
        print("Se detectaron cambios en los registros, se ejecutarán los notebooks.")

    for notebook in notebooks:
        print(f"Checking notebook: {notebook}")
        if os.path.exists(notebook):
            run_notebook(notebook)
        else:
            print(f"Notebook {notebook} not found.")

if __name__ == "__main__":
    main()
