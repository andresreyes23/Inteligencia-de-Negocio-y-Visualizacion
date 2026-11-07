# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05 – Informe de Resultados y Recomendaciones
# MAGIC ## Caso Práctico ElectroTech – MDCBA205
# MAGIC
# MAGIC **Alumno:** [Tu nombre]
# MAGIC **Asignatura:** Inteligencia de Negocio y Visualización (MDCBA205)
# MAGIC **Maestría:** Ciencia de Datos para Negocios (Big DATA & BUSINESS ANALYTICS)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Contexto
# MAGIC ElectroTech es una empresa de venta online de productos electrónicos. Se construyó
# MAGIC un Data Warehouse con arquitectura Medallion (Bronze → Silver → Gold) sobre Databricks
# MAGIC con Delta Lake para analizar sus datos de ventas, inventario, clientes y productos.
# MAGIC
# MAGIC **Período analizado:** Enero – Septiembre 2023

# COMMAND ----------
# MAGIC %md
# MAGIC ## Arquitectura implementada
# MAGIC
# MAGIC ```
# MAGIC FUENTE               BRONZE              SILVER              GOLD
# MAGIC ──────           ──────────────      ─────────────       ────────────────
# MAGIC ventas.csv   →   ventas_raw          ventas              fact_ventas
# MAGIC productos.csv →  productos_raw       productos       →   dim_producto
# MAGIC clientes.csv  →  clientes_raw        clientes            dim_cliente
# MAGIC inventario.csv→  inventario_raw      inventario          dim_inventario
# MAGIC                                                          dim_tiempo
# MAGIC ```
# MAGIC
# MAGIC - **Bronze:** Datos crudos sin transformar, preservados como fuente de verdad
# MAGIC - **Silver:** Datos limpios, tipados y enriquecidos (fecha parseada, ingresos calculados, margen)
# MAGIC - **Gold:** Modelo dimensional estrella optimizado para análisis y reportes

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tarea 1: Análisis de Ventas Mensuales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     nombre_mes AS `Mes`,
# MAGIC     SUM(ingreso_total) AS `Ingresos ($)`,
# MAGIC     SUM(cantidad_vendida) AS `Unidades`,
# MAGIC     COUNT(*) AS `Transacciones`
# MAGIC FROM electrotech_gold.fact_ventas
# MAGIC GROUP BY anio, mes, nombre_mes
# MAGIC ORDER BY anio, mes;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Hallazgos
# MAGIC - **Mes con mayor ingreso:** Febrero 2023 con $2,450 (+23% vs Enero)
# MAGIC - **Mes con menor ingreso:** Septiembre 2023 con $320 — caída sostenida desde junio
# MAGIC - **Tendencia general:** Crecimiento inicial en Q1, caída progresiva en Q2-Q3
# MAGIC - **Limitación:** Los datos de muestra (20 transacciones) no permiten confirmar
# MAGIC   estacionalidad. Con datos históricos de múltiples años el análisis sería más robusto.
# MAGIC
# MAGIC ### Recomendación
# MAGIC - Investigar causas de la caída en Q2-Q3: ¿campañas de marketing reducidas?,
# MAGIC   ¿estacionalidad del mercado electrónico?
# MAGIC - Diseñar campañas de reactivación para los meses de mayo-septiembre

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tarea 2: Productos Estrella (Q3 2023)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     categoria AS `Categoría`,
# MAGIC     marca AS `Marca`,
# MAGIC     SUM(cantidad_vendida) AS `Unidades Q3`,
# MAGIC     SUM(ingreso_total) AS `Ingresos Q3 ($)`,
# MAGIC     ROUND(AVG(margen_ganancia_pct), 1) AS `Margen % Q3`
# MAGIC FROM electrotech_gold.fact_ventas
# MAGIC WHERE fecha_venta BETWEEN '2023-07-01' AND '2023-09-30'
# MAGIC GROUP BY categoria, marca
# MAGIC ORDER BY `Ingresos Q3 ($)` DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Hallazgos
# MAGIC - **Por ingresos:** Laptop ($540) > Auriculares ($450) > Tablet ($320) en Q3
# MAGIC - **Por unidades:** Tablet (4) > Laptop (3) > Smartwatch (2) en Q3
# MAGIC - **Mejor combinación:** Laptop — alto ingreso y margen (si se corrige el precio)
# MAGIC
# MAGIC ### Recomendación
# MAGIC - Aumentar stock de **Auriculares** para Q4, son el producto de mayor valor individual
# MAGIC - Desarrollar bundle marketing: Smartphone + Auriculares + Smartwatch

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tarea 3: Optimización de Inventario

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.id_producto AS `ID`,
# MAGIC     p.categoria AS `Categoría`,
# MAGIC     i.cantidad_disponible AS `Stock Actual`,
# MAGIC     i.tiempo_reposicion_dias AS `Días Reposición`,
# MAGIC     i.demanda_diaria AS `Demanda/Día`,
# MAGIC     i.punto_reorden AS `Punto Reorden`,
# MAGIC     ROUND(i.cantidad_disponible / NULLIF(i.demanda_diaria, 0), 0) AS `Días Cobertura`,
# MAGIC     i.estado_inventario AS `Estado`
# MAGIC FROM electrotech_gold.dim_producto p
# MAGIC JOIN electrotech_gold.dim_inventario i ON p.id_producto = i.id_producto
# MAGIC WHERE i.total_vendido > 0
# MAGIC ORDER BY i.demanda_diaria DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Hallazgos
# MAGIC - Todos los productos activos tienen **cobertura de stock suficiente** para el período
# MAGIC - Los productos 106-120 tienen **cero ventas** → inventario inmovilizado
# MAGIC - **Producto con menor cobertura relativa:** Auriculares (102) — 10 unidades, 10 días de reposición
# MAGIC
# MAGIC ### Recomendación
# MAGIC - Implementar **alertas automáticas** cuando stock < punto de reorden × 1.5
# MAGIC - Revisar la estrategia de compra de productos sin movimiento (106-120)
# MAGIC - Calcular el **costo de oportunidad** del inventario inmovilizado

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tarea 4: Top 5 Clientes Frecuentes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.nombre_cliente AS `Cliente`,
# MAGIC     c.ubicacion AS `Ciudad`,
# MAGIC     COUNT(*) AS `N° Compras`,
# MAGIC     SUM(v.ingreso_total) AS `Gasto Total ($)`,
# MAGIC     ROUND(AVG(v.ingreso_total), 0) AS `Ticket Promedio ($)`,
# MAGIC     MAX(v.fecha_venta) AS `Última Compra`
# MAGIC FROM electrotech_gold.fact_ventas v
# MAGIC JOIN electrotech_gold.dim_cliente c ON v.id_cliente = c.id_cliente
# MAGIC GROUP BY c.nombre_cliente, c.ubicacion
# MAGIC ORDER BY `Gasto Total ($)` DESC
# MAGIC LIMIT 5;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Hallazgos
# MAGIC - Top 2 clientes (Andrés Fernández y Valeria González) compraron Auriculares —
# MAGIC   el producto de mayor precio ($450)
# MAGIC - Guillermo Castro compró 4 Smartphones en una sola transacción ($1,000)
# MAGIC - Ningún cliente repitió compra en el período → **tasa de recompra = 0%**
# MAGIC
# MAGIC ### Recomendación
# MAGIC - Crear programa VIP para los top 5 con descuentos exclusivos
# MAGIC - Enviar notificaciones personalizadas de nuevos modelos en su categoría
# MAGIC - Diseñar estrategia de retención para aumentar la tasa de recompra

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tarea 5: Margen de Ganancia por Categoría (últimos 6 meses)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     categoria AS `Categoría`,
# MAGIC     ROUND(AVG(margen_ganancia_pct), 1) AS `Margen Promedio %`,
# MAGIC     SUM(ingreso_total) AS `Ingresos ($)`,
# MAGIC     SUM(ganancia_neta) AS `Ganancia Neta ($)`,
# MAGIC     CASE
# MAGIC         WHEN AVG(margen_ganancia_pct) >= 30 THEN '🟢 Alto'
# MAGIC         WHEN AVG(margen_ganancia_pct) >= 15 THEN '🟡 Medio'
# MAGIC         ELSE '🔴 Bajo – Revisar'
# MAGIC     END AS `Clasificación`
# MAGIC FROM electrotech_gold.fact_ventas
# MAGIC WHERE fecha_venta BETWEEN '2023-04-01' AND '2023-09-30'
# MAGIC GROUP BY categoria
# MAGIC ORDER BY `Margen Promedio %` DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Hallazgos
# MAGIC - **🟢 Alto margen:** Auriculares (33.3%), Smartwatch (33.3%)
# MAGIC - **🟡 Medio:** Smartphone (28%), Tablet (12.5%)
# MAGIC - **🔴 Revisar:** Laptop — precio de venta en datos ($180) es menor al precio de compra ($500),
# MAGIC   lo que genera margen negativo. Posible error en datos de muestra.
# MAGIC
# MAGIC ### Recomendación
# MAGIC - Enfocar el portafolio de ventas en **Auriculares y Smartwatches** (mejor margen)
# MAGIC - Revisar y corregir la estrategia de precios de **Laptops** urgentemente
# MAGIC - Considerar reducir el costo de adquisición de **Tablets** para mejorar su margen

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Conclusión General
# MAGIC
# MAGIC ElectroTech tiene oportunidades claras de mejora en tres frentes:
# MAGIC
# MAGIC 1. **Rentabilidad:** Los márgenes son altos en Auriculares y Smartwatches;
# MAGIC    hay que resolver el problema de precios en Laptops.
# MAGIC
# MAGIC 2. **Inventario:** 15 de 20 productos no tienen ventas registradas.
# MAGIC    Esto representa capital inmovilizado que reduce la rentabilidad operativa.
# MAGIC
# MAGIC 3. **Retención de clientes:** La tasa de recompra es 0% en el período analizado.
# MAGIC    Implementar programas de fidelización es la acción de mayor impacto potencial.
# MAGIC
# MAGIC **Nota sobre los datos:** El análisis se realizó con 20 transacciones de muestra.
# MAGIC Las conclusiones son indicativas; con datos reales (miles de transacciones) el nivel
# MAGIC de confianza estadística aumentaría significativamente.

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════╗
║      INFORME ELECTROTECH – COMPLETADO                    ║
║      Caso Práctico 205 – MDCBA205                        ║
╠══════════════════════════════════════════════════════════╣
║  Arquitectura: Medallion (Bronze → Silver → Gold)        ║
║  Motor:        Databricks + Delta Lake + PySpark SQL     ║
║  Tareas:       5/5 completadas                           ║
╚══════════════════════════════════════════════════════════╝
""")
