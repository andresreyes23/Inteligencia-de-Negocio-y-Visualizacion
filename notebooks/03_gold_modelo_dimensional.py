# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03 – Gold Layer: Modelo Dimensional (Data Warehouse)
# MAGIC ## Caso Práctico ElectroTech – MDCBA205
# MAGIC
# MAGIC **Objetivo:** Construir el modelo dimensional (esquema estrella) listo para análisis.
# MAGIC
# MAGIC ## Modelo Estrella
# MAGIC
# MAGIC ```
# MAGIC                  dim_tiempo
# MAGIC                      |
# MAGIC  dim_cliente ── fact_ventas ── dim_producto
# MAGIC                      |
# MAGIC                 dim_inventario
# MAGIC ```
# MAGIC
# MAGIC | Tabla | Tipo | Descripción |
# MAGIC |-------|------|-------------|
# MAGIC | fact_ventas | Hecho | Transacciones de ventas con métricas |
# MAGIC | dim_producto | Dimensión | Catálogo de productos |
# MAGIC | dim_cliente | Dimensión | Catálogo de clientes |
# MAGIC | dim_tiempo | Dimensión | Calendario |
# MAGIC | dim_inventario | Dimensión | Estado del inventario |

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 1: Leer tablas Silver

# COMMAND ----------

from pyspark.sql import functions as F

df_ventas    = spark.table("electrotech_silver.ventas")
df_productos = spark.table("electrotech_silver.productos")
df_clientes  = spark.table("electrotech_silver.clientes")
df_inventario = spark.table("electrotech_silver.inventario")

print("Tablas Silver leídas ✓")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 2: Construir dimensiones

# COMMAND ----------

# ─── dim_producto ────────────────────────────────────────────────────────────
dim_producto = (
    df_productos
    .select(
        F.col("id_producto"),
        F.col("categoria"),
        F.col("marca"),
        F.col("precio_compra"),
    )
    .distinct()
)

print("dim_producto:")
dim_producto.show()

# COMMAND ----------

# ─── dim_cliente ─────────────────────────────────────────────────────────────
dim_cliente = (
    df_clientes
    .select(
        F.col("id_cliente"),
        F.col("nombre_cliente"),
        F.col("ubicacion"),
    )
    .distinct()
)

print("dim_cliente:")
dim_cliente.show()

# COMMAND ----------

# ─── dim_tiempo ──────────────────────────────────────────────────────────────
# Genera una fila por cada fecha única en las ventas
dim_tiempo = (
    df_ventas
    .select("fecha_venta")
    .distinct()
    .withColumn("anio",           F.year("fecha_venta"))
    .withColumn("mes_num",        F.month("fecha_venta"))
    .withColumn("nombre_mes",     F.date_format("fecha_venta", "MMMM"))
    .withColumn("trimestre",      F.concat(F.lit("Q"), F.quarter("fecha_venta")))
    .withColumn("semestre",       F.when(F.month("fecha_venta") <= 6, "S1").otherwise("S2"))
    .withColumn("nombre_anio_mes",F.date_format("fecha_venta", "MMM yyyy"))
    .orderBy("fecha_venta")
)

print("dim_tiempo:")
dim_tiempo.show()

# COMMAND ----------

# ─── dim_inventario ───────────────────────────────────────────────────────────
# Calculamos el punto de reorden usando la demanda histórica
# Período total de ventas: 270 días (1 ene – 28 sep 2023)
DIAS_PERIODO = 270

# Demanda total por producto
demanda_por_producto = (
    df_ventas
    .groupBy("id_producto")
    .agg(F.sum("cantidad_vendida").alias("total_vendido"))
)

dim_inventario = (
    df_inventario
    .join(demanda_por_producto, on="id_producto", how="left")
    .withColumn("total_vendido", F.coalesce(F.col("total_vendido"), F.lit(0)))
    # Demanda promedio diaria
    .withColumn(
        "demanda_diaria",
        F.round(F.col("total_vendido") / F.lit(DIAS_PERIODO), 4)
    )
    # Punto de reorden = demanda_diaria × tiempo_reposicion
    .withColumn(
        "punto_reorden",
        F.round(F.col("demanda_diaria") * F.col("tiempo_reposicion_dias"), 2)
    )
    # Stock de seguridad (25% extra sobre el punto de reorden)
    .withColumn(
        "stock_seguridad",
        F.round(F.col("punto_reorden") * 1.25, 2)
    )
    # Alerta: si el stock disponible está por debajo del punto de reorden
    .withColumn(
        "estado_inventario",
        F.when(
            F.col("cantidad_disponible") <= F.col("punto_reorden"),
            "⚠️ REPONER"
        ).otherwise("✅ OK")
    )
    .select(
        "id_producto",
        "cantidad_disponible",
        "tiempo_reposicion_dias",
        "total_vendido",
        "demanda_diaria",
        "punto_reorden",
        "stock_seguridad",
        "estado_inventario",
    )
)

print("dim_inventario con análisis de reorden:")
dim_inventario.show(20, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 3: Construir la tabla de hechos (fact_ventas)

# COMMAND ----------

# fact_ventas: tabla central del modelo estrella
# Contiene todas las métricas de las transacciones de venta
fact_ventas = (
    df_ventas
    .select(
        # Claves foráneas (links a dimensiones)
        "fecha_venta",
        "id_producto",
        "id_cliente",
        # Métricas
        "cantidad_vendida",
        "precio_unitario",
        "precio_compra",
        "ingreso_total",
        "ganancia_neta",
        "margen_ganancia_pct",
        # Atributos de tiempo (desnormalizados para facilitar queries)
        "anio",
        "mes",
        "nombre_mes",
        "trimestre",
        # Atributos de producto (desnormalizados)
        "categoria",
        "marca",
    )
)

print(f"fact_ventas: {fact_ventas.count()} registros")
fact_ventas.show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 4: Guardar en la capa Gold

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS electrotech_gold")
print("Base de datos electrotech_gold creada ✓")

# COMMAND ----------

tables_to_save = {
    "fact_ventas":    fact_ventas,
    "dim_producto":   dim_producto,
    "dim_cliente":    dim_cliente,
    "dim_tiempo":     dim_tiempo,
    "dim_inventario": dim_inventario,
}

for name, df in tables_to_save.items():
    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"electrotech_gold.{name}"))
    print(f"  ✓ electrotech_gold.{name} ({df.count()} filas)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 5: Verificar el modelo completo

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN electrotech_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar la tabla de hechos completa
# MAGIC SELECT
# MAGIC     fecha_venta,
# MAGIC     trimestre,
# MAGIC     nombre_mes,
# MAGIC     categoria,
# MAGIC     marca,
# MAGIC     cantidad_vendida,
# MAGIC     precio_unitario,
# MAGIC     precio_compra,
# MAGIC     ingreso_total,
# MAGIC     ganancia_neta,
# MAGIC     margen_ganancia_pct
# MAGIC FROM electrotech_gold.fact_ventas
# MAGIC ORDER BY fecha_venta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Resumen del modelo dimensional
# MAGIC SELECT 'fact_ventas'    AS tabla, COUNT(*) AS filas FROM electrotech_gold.fact_ventas    UNION ALL
# MAGIC SELECT 'dim_producto'   AS tabla, COUNT(*) AS filas FROM electrotech_gold.dim_producto   UNION ALL
# MAGIC SELECT 'dim_cliente'    AS tabla, COUNT(*) AS filas FROM electrotech_gold.dim_cliente    UNION ALL
# MAGIC SELECT 'dim_tiempo'     AS tabla, COUNT(*) AS filas FROM electrotech_gold.dim_tiempo     UNION ALL
# MAGIC SELECT 'dim_inventario' AS tabla, COUNT(*) AS filas FROM electrotech_gold.dim_inventario;

# COMMAND ----------

print("""
╔══════════════════════════════════════════╗
║  Gold Layer completada exitosamente      ║
║  Modelo estrella creado en:              ║
║  electrotech_gold.*                      ║
║                                          ║
║  Tablas disponibles:                     ║
║   - fact_ventas                          ║
║   - dim_producto                         ║
║   - dim_cliente                          ║
║   - dim_tiempo                           ║
║   - dim_inventario                       ║
║                                          ║
║  Siguiente paso: ejecutar               ║
║  04_analisis_tareas.py                   ║
╚══════════════════════════════════════════╝
""")
