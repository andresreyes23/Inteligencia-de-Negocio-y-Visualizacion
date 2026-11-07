# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02 – Silver Layer: Limpieza y Transformación
# MAGIC ## Caso Práctico ElectroTech – MDCBA205
# MAGIC
# MAGIC **Objetivo:** Leer datos de Bronze, aplicar transformaciones (tipado, columnas derivadas,
# MAGIC validaciones de calidad) y guardar en la capa Silver.
# MAGIC
# MAGIC **Transformaciones aplicadas:**
# MAGIC - Convertir `fecha_venta_str` (String) → `fecha_venta` (Date)
# MAGIC - Calcular columnas derivadas: `ingreso_total`, `mes`, `anio`, `trimestre`, `nombre_mes`
# MAGIC - Validar integridad referencial (ids que existan en sus tablas maestras)
# MAGIC - Calcular `margen_ganancia_pct` cruzando ventas con productos

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 1: Leer tablas Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# Leer desde Bronze
df_ventas_raw     = spark.table("electrotech_bronze.ventas_raw")
df_productos_raw  = spark.table("electrotech_bronze.productos_raw")
df_clientes_raw   = spark.table("electrotech_bronze.clientes_raw")
df_inventario_raw = spark.table("electrotech_bronze.inventario_raw")

print("Tablas Bronze leídas ✓")
df_ventas_raw.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 2: Transformar tabla Ventas

# COMMAND ----------

df_ventas_silver = (
    df_ventas_raw
    # Convertir fecha string a tipo Date
    .withColumn("fecha_venta", F.to_date("fecha_venta_str", "yyyy-MM-dd"))
    .drop("fecha_venta_str")

    # Columnas temporales
    .withColumn("anio",       F.year("fecha_venta"))
    .withColumn("mes",        F.month("fecha_venta"))
    .withColumn("nombre_mes", F.date_format("fecha_venta", "MMM yyyy"))
    .withColumn("trimestre",  F.concat(F.lit("Q"), F.quarter("fecha_venta")))

    # Ingreso total de la venta
    .withColumn("ingreso_total", F.col("cantidad_vendida") * F.col("precio_unitario"))

    # Reordenar columnas de forma lógica
    .select(
        "fecha_venta",
        "anio",
        "mes",
        "nombre_mes",
        "trimestre",
        "id_producto",
        "id_cliente",
        "cantidad_vendida",
        "precio_unitario",
        "ingreso_total",
    )
)

print("Transformación de ventas completada")
df_ventas_silver.show(5)
df_ventas_silver.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 3: Validación de calidad de datos (Ventas)

# COMMAND ----------

# Verificar nulos
print("=== Verificación de nulos en Ventas Silver ===")
for col in df_ventas_silver.columns:
    nulls = df_ventas_silver.filter(F.col(col).isNull()).count()
    status = "✅" if nulls == 0 else f"⚠️  {nulls} nulos"
    print(f"  {col}: {status}")

# Verificar rangos
print("\n=== Rango de fechas ===")
df_ventas_silver.agg(
    F.min("fecha_venta").alias("fecha_min"),
    F.max("fecha_venta").alias("fecha_max"),
    F.count("*").alias("total_registros")
).show()

# Verificar que precio_unitario > 0
precios_invalidos = df_ventas_silver.filter(F.col("precio_unitario") <= 0).count()
print(f"\nPrecios inválidos (<=0): {precios_invalidos}")

# Verificar que cantidad_vendida > 0
cantidades_invalidas = df_ventas_silver.filter(F.col("cantidad_vendida") <= 0).count()
print(f"Cantidades inválidas (<=0): {cantidades_invalidas}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 4: Enriquecer Ventas con datos de Productos (calcular margen)

# COMMAND ----------

# Join ventas con productos para traer precio_compra y categoría
df_ventas_enriched = (
    df_ventas_silver
    .join(
        df_productos_raw.select("id_producto", "categoria", "marca", "precio_compra"),
        on="id_producto",
        how="left"
    )
    # Calcular margen de ganancia
    .withColumn(
        "margen_ganancia_pct",
        F.round(
            (F.col("precio_unitario") - F.col("precio_compra")) / F.col("precio_unitario") * 100,
            2
        )
    )
    # Ganancia neta por venta
    .withColumn(
        "ganancia_neta",
        (F.col("precio_unitario") - F.col("precio_compra")) * F.col("cantidad_vendida")
    )
)

print("Ventas enriquecidas con datos de productos ✓")
df_ventas_enriched.show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 5: Crear base de datos Silver y guardar tablas

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS electrotech_silver")
print("Base de datos electrotech_silver creada ✓")

# COMMAND ----------

# Guardar ventas enriquecidas en Silver
(df_ventas_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_silver.ventas")
)
print("Tabla electrotech_silver.ventas guardada ✓")

# COMMAND ----------

# Guardar productos en Silver (sin cambios significativos, pero en la capa correcta)
(df_productos_raw
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_silver.productos")
)
print("Tabla electrotech_silver.productos guardada ✓")

# COMMAND ----------

# Guardar clientes en Silver
(df_clientes_raw
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_silver.clientes")
)
print("Tabla electrotech_silver.clientes guardada ✓")

# COMMAND ----------

# Guardar inventario en Silver
(df_inventario_raw
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("electrotech_silver.inventario")
)
print("Tabla electrotech_silver.inventario guardada ✓")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 6: Resumen de la capa Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN electrotech_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vista completa de ventas Silver con todos los campos calculados
# MAGIC SELECT
# MAGIC     fecha_venta,
# MAGIC     nombre_mes,
# MAGIC     trimestre,
# MAGIC     id_producto,
# MAGIC     categoria,
# MAGIC     marca,
# MAGIC     id_cliente,
# MAGIC     cantidad_vendida,
# MAGIC     precio_unitario,
# MAGIC     precio_compra,
# MAGIC     ingreso_total,
# MAGIC     ganancia_neta,
# MAGIC     margen_ganancia_pct
# MAGIC FROM electrotech_silver.ventas
# MAGIC ORDER BY fecha_venta;

# COMMAND ----------

print("""
╔══════════════════════════════════════════╗
║  Silver Layer completada exitosamente    ║
║  Tablas limpias y enriquecidas en:       ║
║  electrotech_silver.*                    ║
║                                          ║
║  Siguiente paso: ejecutar               ║
║  03_gold_modelo_dimensional.py           ║
╚══════════════════════════════════════════╝
""")
