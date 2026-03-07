# Northwind Traders — Análisis SQL

Este proyecto contiene los análisis SQL desarrollados sobre el dataset **Northwind Traders**,
una empresa distribuidora de alimentos y bebidas. Los análisis simulan escenarios reales
de negocio con una progresión de dificultad desde intermedio hasta avanzado+.

---

## 📁 Estructura del proyecto

```
sql-server/northwind/
├── docs/        → requerimientos de cada reto en PDF
├── outputs/     → salidas de cada query en CSV
├── queries/     → scripts SQL documentados
└── README.md
```

---

## 📊 Análisis incluidos

### Reto 1 — Análisis RFM de Clientes

**Archivo:** `queries/01_analisis_rfm_clientes.sql`
**Dificultad:** Intermedio → Avanzado

Calcula métricas de comportamiento de compra por cliente: total de órdenes, revenue neto,
valor promedio por orden, fechas de primera y última compra, días de inactividad y ranking
de revenue dentro de su país.

**Conceptos aplicados:** CTEs encadenadas, `RANK()`, `DATEDIFF()`, `NULLIF()`, `COALESCE()`

---

### Reto 2 — Tendencias de Ventas por Producto

**Archivo:** `queries/02_tendencias_ventas_producto.sql`
**Dificultad:** Avanzado

Analiza la evolución mensual del revenue por producto y categoría, incluyendo crecimiento
mes a mes, revenue acumulado por producto y ranking dentro de cada categoría por mes.

**Conceptos aplicados:** `LAG()`, `DENSE_RANK()`, `SUM() OVER()`, `ROWS BETWEEN`,
`DATETRUNC()`, `CASE` con etiquetas de tendencia

---

### Reto 3 — Crecimiento, Concentración y Churn

**Archivo:** `queries/03_crecimiento_concentracion_churn.sql`
**Dificultad:** Avanzado+

Tres análisis ejecutivos en un solo script para la dirección general:

| Query | Descripción |
|---|---|
| Q1 — Crecimiento mensual | Evolución del revenue con promedio móvil de 3 meses |
| Q2 — Concentración HHI | Índice de dependencia de clientes clave |
| Q3 — Detección de Churn | Clasificación de clientes por riesgo de abandono |

**Conceptos aplicados:** `ROWS BETWEEN UNBOUNDED PRECEDING`, `NTILE()`, `SQUARE()`,
`DATEDIFF(DAY) / 30.0`, promedio móvil, acumulados históricos

---

## 📄 Salidas

| Archivo | Descripción |
|---|---|
| `reto_01_analisis_rfm_clientes.csv` | 1 fila por cliente con métricas RFM y ranking por país |
| `reto_02_tendencias_ventas_producto.csv` | 1 fila por producto/mes con tendencia y ranking |
| `reto_03_crecimiento_mensual.csv` | Evolución mensual del revenue de la empresa |
| `reto_03_concentracion_clientes_hhi.csv` | Concentración de revenue por cliente con índice HHI |
| `reto_03_deteccion_churn_clientes.csv` | Clasificación de clientes por riesgo de abandono |

---

## 🛠️ Stack técnico

- **Motor:** SQL Server
- **Dataset:** Northwind (tablas: `Orders`, `Order Details`, `Products`, `Categories`, `Customers`)
- **Funciones destacadas:** CTEs, Window Functions, Date Intelligence, Aggregations
