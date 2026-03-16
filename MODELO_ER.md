# 📊 Modelo Entidad-Relación (ER) - Sistema de Ventas

## 📐 Diagrama ER (Star Schema)

```
┌─────────────────────────────────────┐
│        dim_customer                 │
├─────────────────────────────────────┤
│ PK: customer_id (VARCHAR 50)        │
│     customer_name (VARCHAR 200)     │
└──────────┬──────────────────────────┘
           │
           │ (1:N)
           │
┌──────────▼──────────────────────────┐
│        fact_sales                   │
├─────────────────────────────────────┤
│ PK: sale_id (INT AUTO_INCREMENT)    │
│ FK: customer_id → dim_customer      │
│ FK: product_id → dim_product        │
│ FK: date_id → dim_date              │
│     qty (INT)                       │
│     total (DECIMAL 10,2)            │
│     status (VARCHAR 50)             │
└──────────┬──────────────────────────┘
           │
      ┌────┴────┐
      │         │
   (N:1)     (N:1)
      │         │
      ▼         ▼
┌──────────┐ ┌──────────────────────────┐
│dim_date  │ │   dim_product            │
├──────────┤ ├──────────────────────────┤
│PK: date_ │ │PK: product_id (INT AI)   │
│    id(INT)│ │    item_description(TEXT)│
│date(DATE)│ │                          │
│month(INT)│ │                          │
│year(INT) │ │                          │
└──────────┘ └──────────────────────────┘
```

---

## 📋 Descripción de Tablas

### 1. **dim_customer** (Dimensión de Clientes)
| Campo | Tipo | Restricción | Descripción |
|-------|------|-------------|-------------|
| customer_id | VARCHAR(50) | PRIMARY KEY | Identificador único del cliente |
| customer_name | VARCHAR(200) | NOT NULL | Nombre del cliente |

**Propósito**: Almacenar información de clientes únicos.

---

### 2. **dim_product** (Dimensión de Productos)
| Campo | Tipo | Restricción | Descripción |
|-------|------|-------------|-------------|
| product_id | INT | PRIMARY KEY, AUTO_INCREMENT | Identificador único del producto |
| item_description | TEXT | - | Descripción del producto |

**Propósito**: Almacenar catálogo de productos.

---

### 3. **dim_date** (Dimensión de Fechas)
| Campo | Tipo | Restricción | Descripción |
|-------|------|-------------|-------------|
| date_id | INT | PRIMARY KEY | Identificador único de la fecha |
| date | DATE | - | Fecha completa |
| month | INT | - | Mes (1-12) |
| year | INT | - | Año (YYYY) |

**Propósito**: Optimizar análisis temporales sin recalcular fechas.

---

### 4. **fact_sales** (Tabla de Hechos - Ventas)
| Campo | Tipo | Restricción | Descripción |
|-------|------|-------------|-------------|
| sale_id | INT | PRIMARY KEY, AUTO_INCREMENT | Identificador único de venta |
| customer_id | VARCHAR(50) | FOREIGN KEY | Referencia al cliente |
| product_id | INT | FOREIGN KEY | Referencia al producto |
| date_id | INT | FOREIGN KEY | Referencia a la fecha |
| qty | INT | - | Cantidad vendida |
| total | DECIMAL(10,2) | - | Monto total de la venta |
| status | VARCHAR(50) | - | Estado de la venta (Pago, Pendiente) |

**Propósito**: Registrar transacciones de ventas con referencias a dimensiones.

---

## 🔑 Relaciones

| Relación | Tipo | Descripción |
|----------|------|-------------|
| fact_sales → dim_customer | N:1 | Muchas ventas por cliente |
| fact_sales → dim_product | N:1 | Muchas ventas por producto |
| fact_sales → dim_date | N:1 | Muchas ventas por fecha |

---

## 💡 Características del Modelo

✅ **Star Schema** - Diseño dimensional simple y eficiente  
✅ **Desnormalizado** - Optimizado para consultas analíticas  
✅ **Escalable** - Soporta crecimiento de datos  
✅ **Integridad referencial** - Claves foráneas validadas  
✅ **Análisis rápido** - Joins simples entre hechos y dimensiones  

---

## 🔄 Ciclo de Datos

1. **Ingesta** → CSV → ETL
2. **Limpieza** → Validación, transformación
3. **Carga** → Inserción en tablas dimensionales y hechos
4. **Consulta** → API REST
5. **Visualización** → Dashboard interactivo

---

## 📊 Ejemplos de Análisis Posibles

- **Ventas por mes/año**
- **Top 5 clientes por facturación**
- **Monto pendiente de cobro**
- **Productos más vendidos**
- **Análisis de tendencias temporales**
