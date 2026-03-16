-- ============================================================================
-- SCRIPTS SQL - SISTEMA DE GESTIÓN DE VENTAS (Star Schema)
-- Base de Datos: Sales_db
-- Motor: MySQL 8.0+
-- ============================================================================

-- ============================================================================
-- 1. CREAR BASE DE DATOS
-- ============================================================================
CREATE DATABASE IF NOT EXISTS Sales_db;
USE Sales_db;

-- ============================================================================
-- 2. CREAR TABLA DE DIMENSIÓN: CLIENTES
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id VARCHAR(50) PRIMARY KEY COMMENT 'Identificador único del cliente',
  customer_name VARCHAR(200) NOT NULL COMMENT 'Nombre completo del cliente',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Fecha de creación del registro',
  INDEX idx_customer_name (customer_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Dimensión: Clientes únicos';

-- ============================================================================
-- 3. CREAR TABLA DE DIMENSIÓN: PRODUCTOS
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_product (
  product_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Identificador único del producto',
  item_description TEXT NOT NULL COMMENT 'Descripción del producto',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Fecha de creación del producto',
  INDEX idx_product_desc (item_description(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Dimensión: Productos';

-- ============================================================================
-- 4. CREAR TABLA DE DIMENSIÓN: FECHAS
-- ============================================================================
CREATE TABLE IF NOT EXISTS dim_date (
  date_id INT PRIMARY KEY COMMENT 'Identificador único de la fecha (YYYYMMDD)',
  date DATE NOT NULL UNIQUE COMMENT 'Fecha completa',
  day INT COMMENT 'Día del mes (1-31)',
  month INT COMMENT 'Mes (1-12)',
  year INT COMMENT 'Año (YYYY)',
  quarter INT COMMENT 'Trimestre (1-4)',
  week_of_year INT COMMENT 'Semana del año (1-52)',
  day_of_week INT COMMENT 'Día de la semana (1=Lunes, 7=Domingo)',
  is_weekend BOOLEAN COMMENT 'Indicador de fin de semana',
  INDEX idx_date (date),
  INDEX idx_year_month (year, month),
  INDEX idx_quarter (quarter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Dimensión: Fechas (para análisis temporales optimizados)';

-- ============================================================================
-- 5. CREAR TABLA DE HECHOS: VENTAS
-- ============================================================================
CREATE TABLE IF NOT EXISTS fact_sales (
  sale_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Identificador único de la venta',
  customer_id VARCHAR(50) NOT NULL COMMENT 'Referencia al cliente',
  product_id INT NOT NULL COMMENT 'Referencia al producto',
  date_id INT NOT NULL COMMENT 'Referencia a la fecha de venta',
  qty INT NOT NULL DEFAULT 1 COMMENT 'Cantidad vendida',
  total DECIMAL(12,2) NOT NULL COMMENT 'Monto total de la venta (ARS)',
  status VARCHAR(50) NOT NULL DEFAULT 'Pago' COMMENT 'Estado: Pago, Pendiente',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Fecha de creación del registro',
  
  -- Restricciones de integridad referencial
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id) 
    ON DELETE RESTRICT ON UPDATE CASCADE,
  FOREIGN KEY (product_id) REFERENCES dim_product(product_id) 
    ON DELETE RESTRICT ON UPDATE CASCADE,
  FOREIGN KEY (date_id) REFERENCES dim_date(date_id) 
    ON DELETE RESTRICT ON UPDATE CASCADE,
  
  -- Índices para optimización de consultas
  INDEX idx_customer_id (customer_id),
  INDEX idx_product_id (product_id),
  INDEX idx_date_id (date_id),
  INDEX idx_status (status),
  INDEX idx_created_at (created_at),
  INDEX idx_sales_range (date_id, status)
  
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Tabla de Hechos: Transacciones de ventas';

-- ============================================================================
-- 6. CREAR VISTAS PARA ANÁLISIS COMÚN
-- ============================================================================

-- Vista: Ventas por mes
CREATE OR REPLACE VIEW vw_sales_by_month AS
SELECT 
  dd.year,
  dd.month,
  dd.date_id,
  COUNT(*) as cantidad_transacciones,
  SUM(fs.qty) as cantidad_vendida,
  SUM(fs.total) as total_vendido,
  AVG(fs.total) as ticket_promedio
FROM fact_sales fs
JOIN dim_date dd ON fs.date_id = dd.date_id
GROUP BY dd.year, dd.month
ORDER BY dd.year DESC, dd.month DESC;

-- Vista: Top clientes por facturación
CREATE OR REPLACE VIEW vw_top_customers AS
SELECT 
  dc.customer_id,
  dc.customer_name,
  COUNT(fs.sale_id) as cantidad_compras,
  SUM(fs.qty) as cantidad_vendida,
  SUM(fs.total) as total_facturado,
  AVG(fs.total) as compra_promedio
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_id, dc.customer_name
ORDER BY total_facturado DESC;

-- Vista: Monto pendiente de cobro
CREATE OR REPLACE VIEW vw_pending_payments AS
SELECT 
  COUNT(DISTINCT fs.customer_id) as cantidad_deudores,
  COUNT(fs.sale_id) as cantidad_facturas_pendientes,
  SUM(fs.total) as monto_pendiente_total,
  AVG(fs.total) as factura_promedio_pendiente
FROM fact_sales fs
WHERE fs.status = 'Pendiente';

-- Vista: Productos más vendidos
CREATE OR REPLACE VIEW vw_top_products AS
SELECT 
  dp.product_id,
  dp.item_description,
  COUNT(fs.sale_id) as cantidad_ventas,
  SUM(fs.qty) as cantidad_vendida,
  SUM(fs.total) as ingresos_totales
FROM fact_sales fs
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.product_id, dp.item_description
ORDER BY ingresos_totales DESC;

-- ============================================================================
-- 7. CREAR PROCEDIMIENTOS ALMACENADOS
-- ============================================================================

-- Procedimiento: Insertar rango de fechas en dim_date
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS sp_populate_dates(
  IN p_start_date DATE,
  IN p_end_date DATE
)
BEGIN
  DECLARE v_current_date DATE;
  DECLARE v_date_id INT;
  
  SET v_current_date = p_start_date;
  
  WHILE v_current_date <= p_end_date DO
    SET v_date_id = YEAR(v_current_date) * 10000 + 
                    MONTH(v_current_date) * 100 + 
                    DAY(v_current_date);
    
    INSERT IGNORE INTO dim_date (
      date_id, date, day, month, year, quarter, 
      week_of_year, day_of_week, is_weekend
    ) VALUES (
      v_date_id,
      v_current_date,
      DAY(v_current_date),
      MONTH(v_current_date),
      YEAR(v_current_date),
      QUARTER(v_current_date),
      WEEK(v_current_date),
      DAYOFWEEK(v_current_date),
      DAYOFWEEK(v_current_date) IN (1, 7)
    );
    
    SET v_current_date = DATE_ADD(v_current_date, INTERVAL 1 DAY);
  END WHILE;
END //
DELIMITER ;

-- ============================================================================
-- 8. INICIALIZAR DIMENSIÓN DE FECHAS
-- ============================================================================
-- Ejecutar después de crear el procedimiento (comentado para seguridad)
-- CALL sp_populate_dates('2023-01-01', '2026-12-31');

-- ============================================================================
-- 9. CREAR ÍNDICES ADICIONALES PARA PERFORMANCE
-- ============================================================================
ALTER TABLE fact_sales ADD INDEX idx_total (total);
ALTER TABLE fact_sales ADD INDEX idx_qty (qty);
ALTER TABLE dim_customer ADD UNIQUE INDEX ux_customer_id (customer_id);

-- ============================================================================
-- 10. SENTENCIAS DE PRUEBA / VALIDACIÓN
-- ============================================================================

-- Contar registros en cada tabla
-- SELECT 'dim_customer' as tabla, COUNT(*) as cantidad FROM dim_customer
-- UNION ALL
-- SELECT 'dim_product', COUNT(*) FROM dim_product
-- UNION ALL
-- SELECT 'dim_date', COUNT(*) FROM dim_date
-- UNION ALL
-- SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- ============================================================================
-- 11. MANTENCIÓN Y LIMPIEZA
-- ============================================================================

-- Análisis de tablas
-- ANALYZE TABLE dim_customer;
-- ANALYZE TABLE dim_product;
-- ANALYZE TABLE dim_date;
-- ANALYZE TABLE fact_sales;

-- Optimizar tablas
-- OPTIMIZE TABLE dim_customer;
-- OPTIMIZE TABLE dim_product;
-- OPTIMIZE TABLE dim_date;
-- OPTIMIZE TABLE fact_sales;

-- ============================================================================
-- FIN DEL SCRIPT
-- ============================================================================
-- Fecha de creación: 2026
-- Sistema: Sistema de Gestión de Ventas
-- Versión: 1.0
-- ============================================================================
