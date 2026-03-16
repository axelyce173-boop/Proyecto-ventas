"""API REST para exposición de métricas de ventas.

Endpoints expuestos:
- /metrics/sales_by_month -> ventas totales agrupadas por año/mes
- /metrics/top_customers -> top 5 clientes por facturación
- /metrics/pending -> monto pendiente de cobro y deudores

La API usa MySQL para leer las tablas dimensionales generadas por el ETL.

Swagger: /docs
OpenAPI: /openapi.json
"""

import os

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import List, Dict, Optional


# Ajusta esta URL según tu motor de base de datos.
# Ejemplo MySQL: mysql+pymysql://user:pass@host:3306/dbname
# Ejemplo SQLite: sqlite:///database/data.db
DB_URL = os.environ.get("DATABASE_URL", "mysql+pymysql://root:@localhost:3307/Sales_db")


def get_engine() -> Engine:
    return create_engine(DB_URL, future=True)

app = FastAPI(
    title="API de Análisis de Ventas",
    description="API para consultar métricas de ventas basadas en los datos procesados por el ETL.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


_engine = get_engine()


def _get_conn():
    return _engine.connect()


@app.get("/salud")
def verificar_salud():
    """Verifica que la API y la base de datos estén accesibles."""

    try:
        with _get_conn() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metricas/ventas_por_mes")
def ventas_por_mes() -> List[Dict[str, Optional[float]]]:
    """Ventas totales agrupadas por año/mes."""

    query = """
    SELECT d.year, d.month, SUM(f.total) AS total
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
    """

    with _get_conn() as conn:
        rows = conn.execute(text(query)).mappings().all()

    return [dict(row) for row in rows]


@app.get("/metricas/top_clientes")
def top_clientes(limite: int = 5) -> List[Dict[str, Optional[float]]]:
    """Top clientes por facturación total."""

    query = """
    SELECT c.customer_name, SUM(f.total) AS total
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id
    ORDER BY total DESC
    LIMIT :lim
    """

    with _get_conn() as conn:
        rows = conn.execute(text(query), {"lim": limite}).mappings().all()

    return [dict(row) for row in rows]


@app.get("/metricas/pendiente")
def monto_pendiente() -> Dict[str, Optional[float]]:
    """Monto pendiente de cobro y número de deudores."""

    query_total = """
    SELECT SUM(total) AS pending_total
    FROM fact_sales
    WHERE lower(status) NOT IN ('paid', 'pagado', 'completado', 'completed')
    """
    query_debtors = """
    SELECT COUNT(DISTINCT customer_id) AS debtors
    FROM fact_sales
    WHERE lower(status) NOT IN ('paid', 'pagado', 'completado', 'completed')
    """

    with _get_conn() as conn:
        pending_total = conn.execute(text(query_total)).scalar() or 0.0
        debtors = conn.execute(text(query_debtors)).scalar() or 0

    return {"pending_total": pending_total, "debtors": debtors}
