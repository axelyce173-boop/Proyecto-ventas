# Proyecto de Ingesta, Limpieza y Visualización de Ventas

Esta solución implementa un flujo end-to-end para procesar datos de ventas de un sistema legacy con problemas de calidad. Incluye:

- **Ingesta y limpieza** (ETL)
- **Modelado dimensional** (star schema) en MySQL
- **API REST documentada** (FastAPI / OpenAPI)
- **Dashboard de visualización** (Chart.js)

---

## 1) Enfoque general

### Elección de tecnología
- **Lenguaje**: Python (rápido para prototipos ETL, APIs y visualización). Todos los componentes se pueden ejecutar con `python`.
- **Base de datos**: **MySQL**. Elegí MySQL porque es robusto para producción, permite concurrencia y escalabilidad. El esquema es compatible con otros RDBMS como PostgreSQL.

### Estructura de proyecto
- `etl/limpieza.py`: script principal de ETL (ingesta, limpieza, modelado y carga).
- `database/`: contiene el esquema dimensional (DDL) para MySQL.
- `api/main.py`: API REST con endpoints útiles y documentación automática (`/docs`).
- `dashboard/index.html`: visualización ligera usando Chart.js consumiendo la API.
- `clean_data.py`: wrapper para ejecutar el ETL fácilmente.

---

## 2) Cómo ejecutar

### 2.1 Preparar datos
1. Coloca el CSV de entrada en `data/data.csv`.
2. Asegúrate de que el CSV tenga al menos estas columnas (si faltan, el ETL las crea):
   - `issue_date`
   - `customer_name`
   - `qty`
   - `unit_price`
   - `total`

### 2.2 Correr ETL (ingesta + carga)

El ETL usa MySQL por defecto. Asegúrate de tener MySQL corriendo en `localhost:3307` con base de datos `Sales_db` y usuario `root` sin contraseña.

```sh
python clean_data.py --input data/data.csv --output data/clean_data.csv --db "mysql+pymysql://root:@localhost:3307/Sales_db"
```

Al finalizar:
- `data/clean_data.csv` contiene el dataset limpio.
- La base de datos MySQL contiene el modelo dimensional cargado.

### 2.3 Ejecutar API

Instala dependencias:

```sh
python -m pip install -r requirements.txt
```

Inicia el servidor:

```sh
uvicorn api.main:app --reload
```

Documentación automática:
- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

---

## 3) Endpoints disponibles

- `GET /salud` → verifica que la API y BD estén accesibles
- `GET /metricas/ventas_por_mes` → total de ventas por año/mes
- `GET /metricas/top_clientes?limite=5` → top clientes por facturación
- `GET /metricas/pendiente` → monto pendiente de cobro y número de deudores

---

## 4) Visualización

Abre `dashboard/index.html` en el navegador. El dashboard consume la API en `http://localhost:8000`.

---

## 5) Justificación técnica resumida

- **Limpieza**: se normaliza fecha a `YYYY-MM-DD`, se eliminan caracteres de moneda, se calcula `total` cuando falta, se eliminan duplicados y se estandariza el nombre de cliente.
- **Modelado**: se utiliza un **esquema dimensional** (dim_date, dim_customer, dim_product, fact_sales) para optimizar consultas analíticas y facilitar agregaciones.
- **Exposición**: FastAPI proporciona documentación automática (Swagger/OpenAPI) y permite escalar a endpoints adicionales según requerimientos.
- **Visualización**: Chart.js permite crear gráficas interactivas en el browser sin dependencias complejas.

---

## 6) Entregables

### Repositorio con código fuente
Todo el código está en este repositorio. Estructura:
- `api/`: código de la API
- `dashboard/`: archivos del dashboard
- `database/`: scripts SQL
- `etl/`: scripts de ETL
- `data/`: datos de entrada y salida

### Modelo ER y scripts SQL

#### 📊 Documentación del Modelo ER
Ver archivo: **[MODELO_ER.md](MODELO_ER.md)**

El modelo utiliza un **esquema estrella (star schema)** para optimizar consultas analíticas:

```
┌─────────────────┐     ┌──────────────────┐
│ dim_customer    │────▶│   fact_sales     │◀────┐
│ PK: customer_id │     │ PK: sale_id      │     │
│ • customer_name │     │ • qty, total     │     │
└─────────────────┘     │ • status         │     │
                        └──────────────────┘     │
                              ▲                   │
                    ┌─────────┴──────────┐       │
                    │                    │       │
              ┌─────▼─────┐      ┌──────▼──────┐
              │ dim_date  │      │ dim_product │
              │ PK: date_ │      │ PK: product_│
              │ id        │      │ • desc      │
              │ • month   │      └─────────────┘
              │ • year    │
              └───────────┘
```

**Tablas Dimensionales:**
- **dim_customer**: Clientes únicos
- **dim_product**: Catálogo de productos  
- **dim_date**: Fechas para análisis temporal optimizados
- **fact_sales**: Transacciones (tabla de hechos)

#### 📋 Scripts SQL Completos
Ver archivo: **[database/SCRIPTS_SQL_COMPLETOS.sql](database/SCRIPTS_SQL_COMPLETOS.sql)**

Incluye:
- ✅ Creación de base de datos y tablas
- ✅ Definición de claves foráneas e índices
- ✅ Vistas para análisis común (ventas por mes, top clientes, pendientes)
- ✅ Procedimientos almacenados para población de fechas
- ✅ Comentarios en cada campo
- ✅ Sentencias de optimización y mantenimiento

Script SQL simplificado en `database/ddl.sql` (para uso rápido).

### Entregables Completos

#### ✅ Repositorio con Código Fuente
Todo el código está en este repositorio. Estructura:
- **`api/`**: Código de la API REST (FastAPI)
- **`dashboard/`**: Dashboard interactivo (HTML + Chart.js)
- **`database/`**: Scripts SQL y DDL
- **`etl/`**: Scripts de ETL (limpieza y carga)
- **`data/`**: Datos de entrada y salida
- **`MODELO_ER.md`**: Documentación completa del modelo ER
- **`README.md`**: Este archivo (instrucciones y decisiones técnicas)

#### ✅ Modelo ER Documentado
**Archivo**: [MODELO_ER.md](MODELO_ER.md)
- Diagrama visual del star schema
- Descripciones detalladas de cada tabla
- Relaciones y cardinalidades
- Características y beneficios del modelo

#### ✅ Scripts SQL Completos
**Archivo**: [database/SCRIPTS_SQL_COMPLETOS.sql](database/SCRIPTS_SQL_COMPLETOS.sql)
- Creación de base de datos `Sales_db`
- Definición de 4 tablas dimensionales + tabla de hechos
- Índices optimizados para consultas
- Vistas predefinidas para análisis común
- Procedimientos almacenados
- Comentarios detallados en cada sección
- Sentencias de validación y mantenimiento

**Script simplificado**: [database/ddl.sql](database/ddl.sql) (para uso rápido)
