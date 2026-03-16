"""ETL: Ingesta, limpieza y carga a base de datos.

Este script toma un CSV con datos de ventas (sistema legacy) y realiza:
- Perfilamiento básico del dataset
- Normalización de fechas y números
- Gestión de duplicados y valores nulos
- Construcción de un esquema dimensional y carga en SQLite

Uso:
  python etl/limpieza.py --input data/data.csv --output data/clean_data.csv --db database/data.db
"""

import argparse
import hashlib
from pathlib import Path

import pandas as pd
from dateutil import parser as date_parser
from sqlalchemy import (
    Column,
    Date,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


def perfil(df: pd.DataFrame) -> None:
    """Muestra información básica del dataset."""

    print("=== Perfil del dataset ===")
    print(f"Columnas ({len(df.columns)}): {list(df.columns)}")
    print(f"Registros: {len(df)}")
    print("Nulos por columna:")
    print(df.isnull().sum())
    print("Tipos de datos:")
    print(df.dtypes)
    print("Valores únicos por columna (hasta 10):")
    for col in df.columns:
        print(f"  - {col}: {df[col].nunique()} únicos")


def _parse_issue_date(value):
    """Intenta normalizar fechas con distintos formatos y devuelve un Timestamp."""

    if pd.isna(value):
        return pd.NaT

    s = str(value).strip()
    if not s:
        return pd.NaT

    # Eliminar posibles horas y zonas horarias
    s = s.split(" ")[0]

    for dayfirst in (True, False):
        try:
            dt = date_parser.parse(s, dayfirst=dayfirst, yearfirst=True)
            return pd.Timestamp(dt)
        except Exception:
            continue

    return pd.NaT


def limpiar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica transformaciones de limpieza al DataFrame."""

    # Copia para no mutar el DataFrame original
    df = df.copy()

    # Campos clave esperados
    expected_columns = ["invoice_id", "issue_date", "customer_id", "customer_name", "qty", "unit_price", "total", "status"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA

    # Normalización de fecha
    df["issue_date"] = df["issue_date"].apply(_parse_issue_date)
    df["issue_date"] = df["issue_date"].dt.strftime("%Y-%m-%d")

    # Normalización de números y moneda
    def _parse_money(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).strip()
        # eliminar símbolos de moneda y espacios
        s = s.replace("$", "").replace("USD", "").replace("usd", "").replace(",", "")
        s = s.replace(" ", "")
        if s.upper() in {"NULL", "NONE", "NAN", ""}:
            return pd.NA
        try:
            return float(s)
        except Exception:
            return pd.NA

    for col in ["unit_price", "total"]:
        if col in df.columns:
            df[col] = df[col].apply(_parse_money)

    if "qty" in df.columns:
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)

    # Calcular total si falta o está en 0
    if "total" in df.columns and "qty" in df.columns and "unit_price" in df.columns:
        missing_total = df["total"].isna() | (df["total"] == 0)
        df.loc[missing_total, "total"] = (
            df.loc[missing_total, "qty"].fillna(0) * df.loc[missing_total, "unit_price"].fillna(0)
        )

    # Normalizar clientes
    df["customer_name"] = (
        df["customer_name"].astype(str).str.strip().str.lower().replace("nan", pd.NA)
    )
    df["customer_name"] = df["customer_name"].fillna("unknown")

    # Normalizar customer_id si existe
    if "customer_id" in df.columns:
        df["customer_id"] = (
            df["customer_id"].astype(str).str.strip().str.upper().replace("NAN", pd.NA)
        )
        # Si no existe un customer_id válido, generamos uno a partir del nombre
        missing_cust = df["customer_id"].isna()
        df.loc[missing_cust, "customer_id"] = df.loc[missing_cust, "customer_name"].apply(
            lambda n: _crear_id_cliente(n)
        )

    # Normalizar campos de texto adicionales
    for text_col in ["item_description", "status"]:
        if text_col in df.columns:
            df[text_col] = df[text_col].astype(str).str.strip()
            df[text_col] = df[text_col].replace({"nan": pd.NA, "": pd.NA})

    # Normalizar estado de venta
    if "status" in df.columns:
        status_map = {
            "paid": "paid",
            "pagado": "paid",
            "completado": "paid",
            "completed": "paid",
            "pending": "pending",
            "processing": "pending",
            "refunded": "refunded",
            "cancelled": "cancelled",
            "canceled": "cancelled",
        }
        df["status"] = (
            df["status"].astype(str).str.strip().str.lower().map(status_map)
        )
        df["status"] = df["status"].fillna("pending")
    else:
        df["status"] = "pending"

    # Eliminar duplicados (prioriza el último registro por invoice_id)
    if "invoice_id" in df.columns:
        df = df.sort_values(by="invoice_id").drop_duplicates(subset=["invoice_id"], keep="last")

    # Eliminar duplicados exactos (residuales)
    df = df.drop_duplicates()

    return df


def _crear_id_cliente(name: str) -> str:
    """Genera un customer_id estable a partir del nombre del cliente."""

    name = (name or "unknown").strip().lower()
    # Usamos hash truncado para mantener longitud <= 20
    h = hashlib.sha1(name.encode("utf-8")).hexdigest()
    return f"C-{h[:16]}"


def _crear_id_fecha(date_value) -> int:
    """Genera un date_id entero a partir de una fecha en formato YYYY-MM-DD."""

    if pd.isna(date_value):
        return None

    if isinstance(date_value, pd.Timestamp):
        return int(date_value.strftime("%Y%m%d"))

    s = str(date_value).strip()
    if not s or s.upper() in {"NAT", "NONE", "NULL"}:
        return None

    try:
        return int(s.replace("-", ""))
    except Exception:
        return None


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_db_url(db_arg: str) -> str:
    """Normaliza el parámetro de base de datos a una URL SQLAlchemy.

    - Si se pasa una ruta de archivo (sin esquema), se interpreta como SQLite.
    - Si se pasa una URL (mysql+pymysql://...), se devuelve tal cual.
    """

    if "://" in db_arg:
        return db_arg

    # Asumimos SQLite local
    db_path = Path(db_arg)
    _ensure_dir(db_path)
    return f"sqlite:///{db_path.as_posix()}"


def get_engine(db_url: str) -> Engine:
    """Crea un engine SQLAlchemy para la URL de la base de datos."""

    return create_engine(db_url, future=True)


def inicializar_base_datos(db_url: str) -> None:
    """Crea el esquema dimensional en la base de datos si no existe."""

    engine = get_engine(db_url)
    metadata = MetaData()

    dim_customer = Table(
        "dim_customer",
        metadata,
        Column("customer_id", String(50), primary_key=True),
        Column("customer_name", String(200), nullable=False),
    )

    dim_product = Table(
        "dim_product",
        metadata,
        Column("product_id", Integer, primary_key=True, autoincrement=True),
        Column("item_description", Text),
    )

    dim_date = Table(
        "dim_date",
        metadata,
        Column("date_id", Integer, primary_key=True),
        Column("date", Date),
        Column("month", Integer),
        Column("year", Integer),
    )

    fact_sales = Table(
        "fact_sales",
        metadata,
        Column("sale_id", Integer, primary_key=True, autoincrement=True),
        Column("customer_id", String(50)),
        Column("product_id", Integer),
        Column("date_id", Integer),
        Column("qty", Integer),
        Column("total", Float),
        Column("status", String(50)),
        ForeignKeyConstraint(["customer_id"], ["dim_customer.customer_id"]),
        ForeignKeyConstraint(["product_id"], ["dim_product.product_id"]),
        ForeignKeyConstraint(["date_id"], ["dim_date.date_id"]),
    )

    metadata.create_all(engine)


def cargar_a_base_datos(df: pd.DataFrame, db_url: str) -> None:
    """Carga el DataFrame limpio en las tablas dimensionales."""

    engine = get_engine(db_url)

    # Inserción segura para SQL dialects distintos (SQLite vs MySQL)
    dialect = engine.dialect.name
    insert_customer = "INSERT OR IGNORE" if dialect == "sqlite" else "INSERT IGNORE"
    insert_product = insert_customer
    insert_date = insert_customer

    with engine.begin() as conn:
        # Dimensión clientes
        if "customer_id" in df.columns:
            cust_df = df[["customer_id", "customer_name"]].dropna(subset=["customer_id"]).drop_duplicates("customer_id")
            customer_rows = list(cust_df.itertuples(index=False, name=None))
        else:
            customers = df["customer_name"].dropna().unique().tolist()
            customer_rows = [(_crear_id_cliente(c), c) for c in customers]

        if customer_rows:
            conn.execute(text(f"{insert_customer} INTO dim_customer (customer_id, customer_name) VALUES (:c, :n);"),
                         [dict(c=c, n=n) for c, n in customer_rows])

        # Dimensión productos
        products = df.get("item_description")
        if products is not None:
            products = products.fillna("unknown")
            product_rows = [(p,) for p in products.drop_duplicates().tolist()]
            if product_rows:
                conn.execute(text(f"{insert_product} INTO dim_product (item_description) VALUES (:p);"),
                             [dict(p=p) for (p,) in product_rows])

        # Dimensión fechas
        df["date_id"] = df["issue_date"].apply(_crear_id_fecha)
        dates = (
            df[["issue_date", "date_id"]]
            .dropna(subset=["date_id", "issue_date"])
            .drop_duplicates("date_id")
        )

        date_rows = []
        for issue_date, date_id in dates.itertuples(index=False):
            try:
                dt = pd.to_datetime(issue_date, format="%Y-%m-%d", errors="coerce")
                if pd.isna(dt):
                    continue
                date_rows.append({"date_id": date_id, "date": dt.date(), "month": int(dt.month), "year": int(dt.year)})
            except Exception:
                continue

        if date_rows:
            conn.execute(text(f"{insert_date} INTO dim_date (date_id, date, month, year) VALUES (:date_id, :date, :month, :year);"), date_rows)

        # Fact table
        fact_rows = []
        for _, row in df.iterrows():
            cust_id = row.get("customer_id")
            if not cust_id or pd.isna(cust_id):
                cust_id = _crear_id_cliente(row.get("customer_name"))

            product_id = None
            if "item_description" in row and pd.notna(row["item_description"]):
                prod = conn.execute(
                    text("SELECT product_id FROM dim_product WHERE item_description = :item LIMIT 1;"),
                    {"item": row["item_description"]},
                ).fetchone()
                if prod:
                    product_id = prod[0]

            date_id = row.get("date_id")
            qty = int(row.get("qty") or 0)
            total = float(row.get("total") or 0.0)
            status = row.get("status") or "pending"

            fact_rows.append({
                "customer_id": cust_id,
                "product_id": product_id,
                "date_id": date_id,
                "qty": qty,
                "total": total,
                "status": status,
            })

        if fact_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO fact_sales
                        (customer_id, product_id, date_id, qty, total, status)
                    VALUES (:customer_id, :product_id, :date_id, :qty, :total, :status);
                    """
                ),
                fact_rows,
            )


def principal() -> None:
    parser = argparse.ArgumentParser(description="ETL: limpieza y carga de datos de ventas")
    parser.add_argument(
        "--input",
        default="data/data.csv",
        help="Ruta al CSV de entrada (sistema legacy)",
    )
    parser.add_argument(
        "--output",
        default="data/clean_data.csv",
        help="Ruta al CSV limpio de salida",
    )
    parser.add_argument(
        "--db",
        default="database/data.db",
        help=(
            "URL de base de datos (SQLAlchemy) o ruta a archivo SQLite. "
            "Ejemplo SQLite: database/data.db. Ejemplo MySQL: mysql+pymysql://user:pass@host/db"
        ),
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    db_arg = args.db

    if not input_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo de entrada: {input_path}")

    df = pd.read_csv(input_path, low_memory=False)
    perfil(df)

    clean_df = limpiar_dataframe(df)
    clean_df.to_csv(output_path, index=False)
    print(f"Dataset limpio guardado en: {output_path}")

    db_url = _normalize_db_url(db_arg)
    inicializar_base_datos(db_url)
    cargar_a_base_datos(clean_df, db_url)
    print(f"Datos cargados en la base de datos: {db_url}")


if __name__ == "__main__":
    principal()
