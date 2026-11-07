# Inteligencia de Negocio y Visualización
## Caso Práctico 205 – ElectroTech
### Maestría en Ciencia de Datos para Negocios (Big DATA & BUSINESS ANALYTICS)

Este repositorio contiene la solución del caso práctico de la asignatura **Inteligencia de Negocio y Visualización (MDCBA205)**, implementada sobre **Databricks + Delta Lake** con arquitectura Medallion.

---

## Contexto del caso

**ElectroTech** es una empresa de venta online de productos electrónicos. El objetivo es aplicar los conceptos de BI para realizar análisis y tomar decisiones estratégicas sobre sus datos de ventas, inventario, clientes y productos.

---

## Arquitectura implementada

```
FUENTE (CSV)    →   BRONZE          →   SILVER            →   GOLD
─────────────       ──────────────      ─────────────         ──────────────────
ventas.csv      →   ventas_raw      →   ventas            →   fact_ventas
productos.csv   →   productos_raw   →   productos         →   dim_producto
clientes.csv    →   clientes_raw    →   clientes          →   dim_cliente
inventario.csv  →   inventario_raw  →   inventario        →   dim_inventario
                                                          →   dim_tiempo
```

- **Bronze:** datos crudos sin transformar, preservados como fuente de verdad
- **Silver:** datos limpios, tipados y enriquecidos (fecha parseada, ingresos y márgenes calculados)
- **Gold:** modelo estrella optimizado para análisis y reportes

---

## Tareas resueltas

| Tarea | Descripción | Notebook |
|-------|-------------|----------|
| 1 | Análisis de ventas mensuales | `04_analisis_tareas.py` |
| 2 | Identificación de productos estrella Q3 | `04_analisis_tareas.py` |
| 3 | Optimización de inventario y punto de reorden | `04_analisis_tareas.py` |
| 4 | Top 5 clientes frecuentes | `04_analisis_tareas.py` |
| 5 | Margen de ganancia por categoría | `04_analisis_tareas.py` |

---

## Estructura del repositorio

```
├── data/                        # Datasets del caso práctico
│   ├── ventas.csv
│   ├── productos.csv
│   ├── clientes.csv
│   └── inventario.csv
│
├── notebooks/                   # Notebooks Databricks en orden de ejecución
│   ├── 01_bronze_ingesta.py         # Carga de datos → Delta Tables (Bronze)
│   ├── 02_silver_transformacion.py  # Limpieza y enriquecimiento (Silver)
│   ├── 03_gold_modelo_dimensional.py # Modelo estrella (Gold)
│   ├── 04_analisis_tareas.py        # Las 5 tareas del caso con SQL/PySpark
│   └── 05_informe_resultados.py     # Informe ejecutivo con hallazgos
│
├── docs/                        # Documentación adicional
├── results/                     # Resultados exportados
├── requirements.txt
└── README.md
```

---

## Cómo ejecutar en Databricks

### 1. Crear un clúster
- Runtime recomendado: **Databricks 13.3 LTS** o superior
- Tipo: Single Node (suficiente para Community Edition)

### 2. Importar los notebooks
- En Databricks: **Workspace → tu usuario → Import**
- Sube cada archivo `.py` de la carpeta `notebooks/`

### 3. Ejecutar en orden
```
01_bronze_ingesta.py          ← primero
02_silver_transformacion.py
03_gold_modelo_dimensional.py
04_analisis_tareas.py
05_informe_resultados.py      ← último
```

> Los datos se cargan directamente desde el código en el notebook 01,
> no es necesario subir los CSV al DBFS manualmente.

---

## Resultados clave

| Métrica | Valor |
|---------|-------|
| Ingresos totales (Ene-Sep 2023) | $9,900 |
| Mes con mayor ingreso | Febrero 2023 ($2,250) |
| Producto estrella Q3 por ingresos | Laptop ($540) |
| Clientes top (empate) | Andrés Fernández y Valeria González ($1,350 c/u) |
| Mejor margen por categoría | Auriculares y Smartwatch (33.3%) |
| Categoría a revisar | Laptop (margen negativo) |

---

## Tecnologías utilizadas

- **Databricks Community Edition**
- **Apache Spark / PySpark**
- **Delta Lake**
- **Spark SQL**
- **Python 3.x**
