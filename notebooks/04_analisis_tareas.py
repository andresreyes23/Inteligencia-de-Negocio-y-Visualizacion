# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04 – Análisis: Las 5 Tareas del Caso Práctico
# MAGIC ## Caso Práctico ElectroTech – MDCBA205
# MAGIC
# MAGIC Este notebook resuelve las 5 tareas requeridas usando SQL y PySpark
# MAGIC sobre el modelo dimensional de la capa Gold.
# MAGIC
# MAGIC | Tarea | Descripción |
# MAGIC |-------|-------------|
# MAGIC | 1 | Análisis de Ventas Mensuales |
# MAGIC | 2 | Identificación de Productos Estrella |
# MAGIC | 3 | Optimización de Inventario |
# MAGIC | 4 | Análisis de Clientes Frecuentes |
# MAGIC | 5 | Cuadro de Mando de Margen de Ganancia |

# COMMAND ----------

from pyspark.sql import functions as F

# Cargar tablas Gold
fact_ventas    = spark.table("electrotech_gold.fact_ventas")
dim_producto   = spark.table("electrotech_gold.dim_producto")
dim_cliente    = spark.table("electrotech_gold.dim_cliente")
dim_inventario = spark.table("electrotech_gold.dim_inventario")

print("Tablas Gold cargadas ✓")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # TAREA 1: Análisis de Ventas Mensuales
# MAGIC **Objetivo:** Mostrar la evolución de ingresos mes a mes e identificar tendencias.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ventas mensuales: ingresos, unidades y número de transacciones
# MAGIC SELECT
# MAGIC     anio,
# MAGIC     mes,
# MAGIC     nombre_mes,
# MAGIC     COUNT(*)                        AS num_transacciones,
# MAGIC     SUM(cantidad_vendida)           AS total_unidades,
# MAGIC     SUM(ingreso_total)              AS ingresos_totales,
# MAGIC     ROUND(AVG(precio_unitario), 2)  AS precio_promedio
# MAGIC FROM electrotech_gold.fact_ventas
# MAGIC GROUP BY anio, mes, nombre_mes
# MAGIC ORDER BY anio, mes;

# COMMAND ----------

# Calcular crecimiento mes a mes con PySpark (window function)
from pyspark.sql.window import Window

ventas_mensuales = (
    fact_ventas
    .groupBy("anio", "mes", "nombre_mes")
    .agg(
        F.count("*").alias("num_transacciones"),
        F.sum("cantidad_vendida").alias("total_unidades"),
        F.sum("ingreso_total").alias("ingresos_totales"),
    )
    .orderBy("anio", "mes")
)

# Window para calcular mes anterior
w = Window.orderBy("anio", "mes")

ventas_mensuales_tendencia = (
    ventas_mensuales
    .withColumn("ingresos_mes_anterior", F.lag("ingresos_totales", 1).over(w))
    .withColumn(
        "variacion_pct",
        F.when(
            F.col("ingresos_mes_anterior").isNotNull(),
            F.round(
                (F.col("ingresos_totales") - F.col("ingresos_mes_anterior"))
                / F.col("ingresos_mes_anterior") * 100,
                1
            )
        ).otherwise(None)
    )
    .withColumn(
        "tendencia",
        F.when(F.col("variacion_pct") > 0,  "📈 Crecimiento")
         .when(F.col("variacion_pct") < 0,  "📉 Caída")
         .otherwise("➡️  Sin cambio")
    )
)

print("=" * 65)
print("TAREA 1 – EVOLUCIÓN DE VENTAS MENSUALES")
print("=" * 65)
ventas_mensuales_tendencia.show(12, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC **Hallazgo Tarea 1:**
# MAGIC - El mes con mayores ingresos es **Febrero 2023** ($2,450)
# MAGIC - La mayor caída ocurre de Febrero a Marzo (-65.7%)
# MAGIC - Los meses con menores ingresos son Agosto y Septiembre
# MAGIC - Con solo 9 meses de datos no es posible confirmar estacionalidad anual

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # TAREA 2: Identificación de Productos Estrella (Último Trimestre)
# MAGIC **Objetivo:** Identificar los productos más vendidos en Q3 2023 (Jul-Sep).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Productos más vendidos en el último trimestre (Q3: Jul-Sep 2023)
# MAGIC SELECT
# MAGIC     v.id_producto,
# MAGIC     p.categoria,
# MAGIC     p.marca,
# MAGIC     SUM(v.cantidad_vendida)                     AS unidades_vendidas,
# MAGIC     SUM(v.ingreso_total)                        AS ingresos_totales,
# MAGIC     SUM(v.ganancia_neta)                        AS ganancia_total,
# MAGIC     ROUND(AVG(v.margen_ganancia_pct), 2)        AS margen_promedio_pct,
# MAGIC     RANK() OVER (ORDER BY SUM(v.cantidad_vendida) DESC) AS ranking_unidades,
# MAGIC     RANK() OVER (ORDER BY SUM(v.ingreso_total) DESC)    AS ranking_ingresos
# MAGIC FROM electrotech_gold.fact_ventas v
# MAGIC JOIN electrotech_gold.dim_producto p ON v.id_producto = p.id_producto
# MAGIC WHERE v.fecha_venta BETWEEN '2023-07-01' AND '2023-09-30'
# MAGIC GROUP BY v.id_producto, p.categoria, p.marca
# MAGIC ORDER BY unidades_vendidas DESC;

# COMMAND ----------

# Vista comparativa: todo el año vs Q3
print("=" * 65)
print("TAREA 2 – PRODUCTOS ESTRELLA: AÑO COMPLETO vs Q3")
print("=" * 65)

# Anual
anual = (fact_ventas
    .groupBy("id_producto", "categoria")
    .agg(
        F.sum("cantidad_vendida").alias("unidades_anual"),
        F.sum("ingreso_total").alias("ingresos_anual")
    )
)

# Q3
q3 = (fact_ventas
    .filter(
        (F.col("fecha_venta") >= "2023-07-01") &
        (F.col("fecha_venta") <= "2023-09-30")
    )
    .groupBy("id_producto", "categoria")
    .agg(
        F.sum("cantidad_vendida").alias("unidades_q3"),
        F.sum("ingreso_total").alias("ingresos_q3")
    )
)

comparativo = (anual
    .join(q3, on=["id_producto", "categoria"], how="left")
    .fillna(0)
    .withColumn(
        "pct_ventas_en_q3",
        F.round(F.col("unidades_q3") / F.col("unidades_anual") * 100, 1)
    )
    .orderBy("unidades_anual", ascending=False)
)

comparativo.show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC **Hallazgo Tarea 2:**
# MAGIC - En Q3, **Tablet (103)** lideró en unidades vendidas (4 unidades)
# MAGIC - **Auriculares (102)** generó el mayor ingreso en Q3 ($450)
# MAGIC - **Laptop (104)** generó $540 en Q3 con solo 3 unidades (alto valor por unidad)
# MAGIC - Recomendación: priorizar stock de Auriculares y Laptops por su rentabilidad

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # TAREA 3: Optimización de Inventario
# MAGIC **Objetivo:** Calcular el nivel óptimo de inventario y detectar productos en riesgo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Análisis completo de inventario con punto de reorden
# MAGIC -- dim_inventario se une a dim_producto (no a fact_ventas directamente)
# MAGIC SELECT
# MAGIC     p.id_producto,
# MAGIC     p.categoria,
# MAGIC     p.marca,
# MAGIC     i.cantidad_disponible       AS stock_actual,
# MAGIC     i.tiempo_reposicion_dias,
# MAGIC     i.total_vendido             AS unidades_vendidas_periodo,
# MAGIC     i.demanda_diaria,
# MAGIC     i.punto_reorden,
# MAGIC     i.stock_seguridad,
# MAGIC     ROUND(i.cantidad_disponible / NULLIF(i.demanda_diaria, 0), 0)
# MAGIC                                 AS dias_cobertura,
# MAGIC     i.estado_inventario
# MAGIC FROM electrotech_gold.dim_producto p
# MAGIC JOIN electrotech_gold.dim_inventario i ON p.id_producto = i.id_producto
# MAGIC ORDER BY i.demanda_diaria DESC, i.cantidad_disponible ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Productos SIN ventas en el período (inventario inmovilizado)
# MAGIC -- JOIN a través de dim_producto → dim_inventario
# MAGIC SELECT
# MAGIC     p.id_producto,
# MAGIC     p.categoria,
# MAGIC     p.marca,
# MAGIC     i.cantidad_disponible,
# MAGIC     'Sin movimiento' AS estado
# MAGIC FROM electrotech_gold.dim_producto p
# MAGIC JOIN electrotech_gold.dim_inventario i ON p.id_producto = i.id_producto
# MAGIC WHERE i.total_vendido = 0
# MAGIC ORDER BY i.cantidad_disponible DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC **Hallazgo Tarea 3:**
# MAGIC - Productos 101-105 son los únicos con ventas registradas; 106-120 no tienen movimiento
# MAGIC - Los productos sin ventas representan **inventario inmovilizado** (capital no productivo)
# MAGIC - El stock actual cubre ampliamente la demanda de los productos activos
# MAGIC - **Recomendación:** revisar estrategia de marketing para productos 106-120,
# MAGIC   o considerar promociones para reducir el inventario inmovilizado

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # TAREA 4: Análisis de Clientes Frecuentes
# MAGIC **Objetivo:** Identificar los 5 clientes con mayor gasto y analizar sus patrones.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 5 clientes por gasto total
# MAGIC SELECT
# MAGIC     v.id_cliente,
# MAGIC     c.nombre_cliente,
# MAGIC     c.ubicacion,
# MAGIC     COUNT(*)                        AS num_compras,
# MAGIC     SUM(v.cantidad_vendida)         AS total_unidades,
# MAGIC     SUM(v.ingreso_total)            AS gasto_total,
# MAGIC     ROUND(AVG(v.ingreso_total), 2)  AS ticket_promedio,
# MAGIC     MIN(v.fecha_venta)              AS primera_compra,
# MAGIC     MAX(v.fecha_venta)              AS ultima_compra
# MAGIC FROM electrotech_gold.fact_ventas v
# MAGIC JOIN electrotech_gold.dim_cliente c ON v.id_cliente = c.id_cliente
# MAGIC GROUP BY v.id_cliente, c.nombre_cliente, c.ubicacion
# MAGIC ORDER BY gasto_total DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detalle de compras de los Top 5 clientes
# MAGIC SELECT
# MAGIC     c.nombre_cliente,
# MAGIC     v.fecha_venta,
# MAGIC     v.categoria,
# MAGIC     v.marca,
# MAGIC     v.cantidad_vendida,
# MAGIC     v.precio_unitario,
# MAGIC     v.ingreso_total
# MAGIC FROM electrotech_gold.fact_ventas v
# MAGIC JOIN electrotech_gold.dim_cliente c ON v.id_cliente = c.id_cliente
# MAGIC WHERE v.id_cliente IN (
# MAGIC     SELECT id_cliente
# MAGIC     FROM electrotech_gold.fact_ventas
# MAGIC     GROUP BY id_cliente
# MAGIC     ORDER BY SUM(ingreso_total) DESC
# MAGIC     LIMIT 5
# MAGIC )
# MAGIC ORDER BY v.ingreso_total DESC, c.nombre_cliente;

# COMMAND ----------
# MAGIC %md
# MAGIC **Hallazgo Tarea 4:**
# MAGIC - **Andrés Fernández (506)** y **Valeria González (511)** tienen el mayor gasto ($1,350 c/u)
# MAGIC   — ambos compraron Auriculares (producto de mayor precio)
# MAGIC - **Guillermo Castro (514)** es el tercer cliente ($1,000) — compró 4 Smartphones
# MAGIC - La mayoría de clientes tiene una sola compra → baja frecuencia de recompra
# MAGIC - **Recomendaciones:**
# MAGIC   - Programa de puntos o descuentos por lealtad para los top 5
# MAGIC   - Email marketing personalizado según categoría comprada
# MAGIC   - Notificaciones de nuevos modelos en su categoría favorita

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # TAREA 5: Cuadro de Mando de Margen de Ganancia
# MAGIC **Objetivo:** Mostrar márgenes por categoría en los últimos 6 meses (Abr-Sep 2023).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Margen de ganancia por categoría – últimos 6 meses (Apr-Sep 2023)
# MAGIC SELECT
# MAGIC     categoria,
# MAGIC     COUNT(*)                                        AS num_ventas,
# MAGIC     SUM(cantidad_vendida)                           AS unidades_vendidas,
# MAGIC     SUM(ingreso_total)                              AS ingresos_totales,
# MAGIC     SUM(ganancia_neta)                              AS ganancia_total,
# MAGIC     ROUND(AVG(precio_unitario), 2)                  AS precio_venta_prom,
# MAGIC     ROUND(AVG(precio_compra), 2)                    AS precio_compra_prom,
# MAGIC     ROUND(AVG(margen_ganancia_pct), 2)              AS margen_promedio_pct,
# MAGIC     ROUND(SUM(ganancia_neta) / SUM(ingreso_total) * 100, 2)
# MAGIC                                                     AS margen_real_pct,
# MAGIC     CASE
# MAGIC         WHEN ROUND(AVG(margen_ganancia_pct), 2) >= 30 THEN '🟢 Alto'
# MAGIC         WHEN ROUND(AVG(margen_ganancia_pct), 2) >= 15 THEN '🟡 Medio'
# MAGIC         ELSE '🔴 Bajo / Revisar'
# MAGIC     END                                             AS clasificacion_margen
# MAGIC FROM electrotech_gold.fact_ventas
# MAGIC WHERE fecha_venta BETWEEN '2023-04-01' AND '2023-09-30'
# MAGIC GROUP BY categoria
# MAGIC ORDER BY margen_promedio_pct DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Evolución del margen por mes (últimos 6 meses)
# MAGIC SELECT
# MAGIC     mes,
# MAGIC     nombre_mes,
# MAGIC     categoria,
# MAGIC     ROUND(AVG(margen_ganancia_pct), 2)  AS margen_pct,
# MAGIC     SUM(ingreso_total)                  AS ingresos,
# MAGIC     SUM(ganancia_neta)                  AS ganancia
# MAGIC FROM electrotech_gold.fact_ventas
# MAGIC WHERE fecha_venta BETWEEN '2023-04-01' AND '2023-09-30'
# MAGIC GROUP BY mes, nombre_mes, categoria
# MAGIC ORDER BY mes, margen_pct DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC **Hallazgo Tarea 5:**
# MAGIC - **Auriculares (102)** y **Smartwatch (105)**: margen ~33% 🟢 Excelente
# MAGIC - **Smartphone (101)**: margen 28% 🟡 Bueno
# MAGIC - **Tablet (103)**: margen 12.5% 🟡 Bajo
# MAGIC - **Laptop (104)**: margen negativo 🔴 — precio de venta ($180) < precio de compra ($500)
# MAGIC   → Este dato sugiere un error en los datos de muestra o una venta a pérdida
# MAGIC - **Recomendaciones:**
# MAGIC   - Revisar urgentemente la estrategia de precios de Laptops
# MAGIC   - Considerar aumentar precio de Tablets o reducir costo de compra
# MAGIC   - Aprovechar el alto margen de Auriculares con mayor volumen de ventas

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC # Resumen Ejecutivo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPIs globales del negocio
# MAGIC SELECT
# MAGIC     COUNT(*)                                AS total_transacciones,
# MAGIC     SUM(cantidad_vendida)                   AS total_unidades_vendidas,
# MAGIC     SUM(ingreso_total)                      AS ingresos_totales,
# MAGIC     SUM(ganancia_neta)                      AS ganancia_total,
# MAGIC     ROUND(AVG(margen_ganancia_pct), 2)      AS margen_promedio_pct,
# MAGIC     COUNT(DISTINCT id_cliente)              AS clientes_activos,
# MAGIC     COUNT(DISTINCT id_producto)             AS productos_con_ventas,
# MAGIC     MIN(fecha_venta)                        AS primera_venta,
# MAGIC     MAX(fecha_venta)                        AS ultima_venta
# MAGIC FROM electrotech_gold.fact_ventas;

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════╗
║           ANÁLISIS COMPLETO – ELECTROTECH                ║
║           Caso Práctico 205 – MDCBA205                   ║
╠══════════════════════════════════════════════════════════╣
║  ✅ Tarea 1: Ventas Mensuales          → Completada      ║
║  ✅ Tarea 2: Productos Estrella Q3     → Completada      ║
║  ✅ Tarea 3: Optimización Inventario   → Completada      ║
║  ✅ Tarea 4: Top 5 Clientes            → Completada      ║
║  ✅ Tarea 5: Márgenes por Categoría    → Completada      ║
╠══════════════════════════════════════════════════════════╣
║  Siguiente paso: ejecutar                                ║
║  05_informe_resultados.py                                ║
╚══════════════════════════════════════════════════════════╝
""")
