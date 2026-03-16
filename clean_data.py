"""Wrapper sencillo para ejecutar el ETL de limpieza y carga.

Este script es una entrada común para ejecutar el pipeline desde la raíz del proyecto.
"""

import argparse
import subprocess
import sys


def principal() -> None:
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline ETL de limpieza y carga")
    parser.add_argument("--input", default="data/data.csv", help="CSV de entrada")
    parser.add_argument("--output", default="data/clean_data.csv", help="CSV de salida limpio")
    parser.add_argument(
        "--db",
        default="database/data.db",
        help=(
            "URL de base de datos SQLAlchemy o ruta SQLite. "
            "Ej: sqlite:///database/data.db o mysql+pymysql://user:pass@host/db"
        ),
    )
    args = parser.parse_args()

    cmd = [
        sys.executable,
        "etl/limpieza.py",
        "--input",
        args.input,
        "--output",
        args.output,
        "--db",
        args.db,
    ]

    subprocess.check_call(cmd)


if __name__ == "__main__":
    principal()
