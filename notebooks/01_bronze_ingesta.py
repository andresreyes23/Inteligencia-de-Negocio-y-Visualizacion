# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01 – Bronze Layer: Ingesta de Datos
# MAGIC ## Caso Práctico ElectroTech – MDCBA205
# MAGIC
# MAGIC **Objetivo:** Cargar los datos crudos del caso práctico como tablas Delta en la capa Bronze.
# MAGIC
# MAGIC La capa Bronze almacena los datos **tal como vienen de la fuente**, sin transformaciones.
# MAGIC Este es el principio de la arquitectura Medallion (Bronze → Silver → Gold).

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 1: Subir los archivos CSV a Databricks
# MAGIC
# MAGIC Antes de ejecutar este notebook, sube los 4 archivos CSV al DBFS (Databricks File System):
# MAGIC
# MAGIC 1. En el menú izquierdo: **Data → Add Data → Upload Files**
# MAGIC 2. Sube: `ventas.csv`, `productos.csv`, `clientes.csv`, `inventario.csv`
# MAGIC 3. Los archivos quedarán en: `/FileStore/tables/`
# MAGIC
# MAGIC O usa el siguiente código para crearlos directamente desde Python:

# COMMAND ----------

# Opción alternativa: crear los datos directamente en el notebook sin subir archivos
# Útil para Databricks Community Edition

ventas_data = [
    ("2023-01-05", 101, 3, 250, 501),
    ("2023-01-10", 102, 1, 450, 502),
    ("2023-01-15", 105, 2, 120, 503),
    ("2023-02-08", 103, 5,  80, 504),
    ("2023-02-15", 101, 2, 250, 505),
    ("2023-02-25", 102, 3, 450, 506),
    ("2023-03-10", 105, 1, 120, 507),
    ("2023-03-18", 104, 4, 180, 508),
    ("2023-04-02", 101, 2, 250, 509),
    ("2023-04-10", 103, 1,  80, 510),
    ("2023-04-20", 102, 3, 450, 511),
    ("2023-05-05", 104, 2, 180, 512),
    ("2023-05-15", 105, 1, 120, 513),
    ("2023-06-10", 101, 4, 250, 514),
    ("2023-06-20", 103, 2,  80, 515),
    ("2023-07-08", 102, 1, 450, 516),
    ("2023-07-20", 104, 3, 180, 517),
    ("2023-08-05", 105, 2, 120, 518),
    ("2023-08-15", 101, 1, 250, 519),
    ("2023-09-02", 103, 4,  80, 520),
]

productos_data = [
    (101, "Smartphone",   "MarcaX", 180),
    (102, "Auriculares",  "MarcaY", 300),
    (103, "Tablet",       "MarcaZ",  70),
    (104, "Laptop",       "MarcaX", 500),
    (105, "Smartwatch",   "MarcaY",  80),
    (106, "Auriculares",  "MarcaX", 250),
    (107, "Cámara",       "MarcaZ", 200),
    (108, "Smartphone",   "MarcaY", 160),
    (109, "Tablet",       "MarcaX",  90),
    (110, "Laptop",       "MarcaZ", 480),
    (111, "Smartwatch",   "MarcaX", 100),
    (112, "Auriculares",  "MarcaY", 220),
    (113, "Cámara",       "MarcaZ", 180),
    (114, "Smartphone",   "MarcaX", 210),
    (115, "Tablet",       "MarcaY", 110),
    (116, "Laptop",       "MarcaX", 460),
    (117, "Smartwatch",   "MarcaZ",  70),
    (118, "Auriculares",  "MarcaX", 280),
    (119, "Cámara",       "MarcaY", 220),
    (120, "Smartphone",   "MarcaZ", 190),
]

clientes_data = [
    (501, "Ana García",           "Ciudad A"),
    (502, "Juan Pérez",           "Ciudad B"),
    (503, "María López",          "Ciudad C"),
    (504, "Carlos Martínez",      "Ciudad D"),
    (505, "Laura Sánchez",        "Ciudad E"),
    (506, "Andrés Fernández",     "Ciudad F"),
    (507, "Sofía Ramírez",        "Ciudad G"),
    (508, "Diego Torres",         "Ciudad H"),
    (509, "Marta Rodríguez",      "Ciudad I"),
    (510, "Javier Hernández",     "Ciudad J"),
    (511, "Valeria González",     "Ciudad K"),
    (512, "Ernesto Morales",      "Ciudad L"),
    (513, "Patricia Ruiz",        "Ciudad M"),
    (514, "Guillermo Castro",     "Ciudad N"),
    (515, "Adriana Méndez",       "Ciudad O"),
    (516, "Oscar Vargas",         "Ciudad P"),
    (517, "Ana María Fernández",  "Ciudad Q"),
    (518, "Luis Mendoza",         "Ciudad R"),
    (519, "Andrea Jiménez",       "Ciudad S"),
    (520, "David Rivas",          "Ciudad T"),
]

inventario_data = [
    (101, 20,  7), (102, 10, 10), (103, 25,  5), (104, 12,  8), (105, 15,  5),
    (106,  8, 12), (107, 18,  6), (108,  5, 15), (109, 30,  4), (110,  7, 10),
    (111, 22,  7), (112,  9, 12), (113, 14,  8), (114, 16,  6), (115, 11,  9),
    (116, 23,  6), (117,  6, 11), (118, 19,  5), (119, 13,  9), (120, 28,  3),
]

print("Datos cargados en memoria ✓")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 2: Crear DataFrames y guardar como tablas Delta en Bronze

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType
)

# Schema de Ventas
schema_ventas = StructType([
    StructField("fecha_venta_str",  StringType(),  False),
    StructField("id_producto",      IntegerType(), False),
    StructField("cantidad_vendida", IntegerType(), False),
    StructField("precio_unitario",  IntegerType(), False),
    StructField("id_cliente",       IntegerType(), False),
])

# Schema de Productos
schema_productos = StructType([
    StructField("id_producto",    IntegerType(), False),
    StructField("categoria",      StringType(),  False),
    StructField("marca",          StringType(),  False),
    StructField("precio_compra",  IntegerType(), False),
])

# Schema de Clientes
schema_clientes = StructType([
    StructField("id_cliente",      IntegerType(), False),
    StructField("nombre_cliente",  StringType(),  False),
    StructField("ubicacion",       StringType(),  False),
])

# Schema de Inventario
schema_inventario = StructType([
    StructField("id_producto",            IntegerType(), False),
    StructField("cantidad_disponible",    IntegerType(), False),
    StructField("tiempo_reposicion_dias", IntegerType(), False),
])

print("Schemas definidos ✓")

# COMMAND ----------

# Crear DataFrames desde los datos en memoria
df_ventas     = spark.createDataFrame(ventas_data,     schema=schema_ventas)
df_productos  = spark.createDataFrame(productos_data,  schema=schema_productos)
df_clientes   = spark.createDataFrame(clientes_data,   schema=schema_clientes)
df_inventario = spark.createDataFrame(inventario_data, schema=schema_inventario)

# Mostrar conteos para verificar
print(f"Ventas:     {df_ventas.count()} registros")
print(f"Productos:  {df_productos.count()} registros")
print(f"Clientes:   {df_clientes.count()} registros")
print(f"Inventario: {df_inventario.count()} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 3: Crear la base de datos y guardar tablas Bronze (Delta)

# COMMAND ----------

# Crear base de datos si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS electrotech_bronze")
print("Base de datos electrotech_bronze creada ✓")

# COMMAND ----------

# Guardar tabla Bronze: ventas
(df_ventas
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_bronze.ventas_raw")
)
print("Tabla electrotech_bronze.ventas_raw guardada ✓")

# COMMAND ----------

# Guardar tabla Bronze: productos
(df_productos
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_bronze.productos_raw")
)
print("Tabla electrotech_bronze.productos_raw guardada ✓")

# COMMAND ----------

# Guardar tabla Bronze: clientes
(df_clientes
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_bronze.clientes_raw")
)
print("Tabla electrotech_bronze.clientes_raw guardada ✓")

# COMMAND ----------

# Guardar tabla Bronze: inventario
(df_inventario
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_bronze.inventario_raw")
)
print("Tabla electrotech_bronze.inventario_raw guardada ✓")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 4: Verificar las tablas creadas

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN electrotech_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vista previa de cada tabla Bronze
# MAGIC SELECT 'ventas' AS tabla, COUNT(*) AS registros FROM electrotech_bronze.ventas_raw
# MAGIC UNION ALL
# MAGIC SELECT 'productos',        COUNT(*) FROM electrotech_bronze.productos_raw
# MAGIC UNION ALL
# MAGIC SELECT 'clientes',         COUNT(*) FROM electrotech_bronze.clientes_raw
# MAGIC UNION ALL
# MAGIC SELECT 'inventario',       COUNT(*) FROM electrotech_bronze.inventario_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM electrotech_bronze.ventas_raw LIMIT 5;

# COMMAND ----------

print("""
╔══════════════════════════════════════════╗
║  Bronze Layer completada exitosamente    ║
║  4 tablas Delta creadas en:              ║
║  electrotech_bronze.*_raw                ║
║                                          ║
║  Siguiente paso: ejecutar               ║
║  02_silver_transformacion.py             ║
╚══════════════════════════════════════════╝
""")
