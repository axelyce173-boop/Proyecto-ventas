-- Esquema dimensional (star schema) para MySQL.
-- Se asume que la base ya existe y el usuario tiene permisos.

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id VARCHAR(50) PRIMARY KEY,
  customer_name VARCHAR(200) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_id INT AUTO_INCREMENT PRIMARY KEY,
  item_description TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_id INT PRIMARY KEY,
  date DATE,
  month INT,
  year INT
);

CREATE TABLE IF NOT EXISTS fact_sales (
  sale_id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id VARCHAR(50),
  product_id INT,
  date_id INT,
  qty INT,
  total DECIMAL(10,2),
  status VARCHAR(50),
  FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
  FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
  FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);
