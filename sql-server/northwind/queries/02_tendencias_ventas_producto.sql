/*
================================================================================
  ANÁLISIS DE VENTAS MENSUAL POR PRODUCTO Y CATEGORÍA — Northwind Traders
  Versión     : 2.0 (production-ready)
  Autor       : [Tu nombre]
  Fecha       : 2026-03-05
================================================================================
  Descripción : Calcula el revenue y órdenes mensuales por producto, junto con
                métricas de crecimiento mes a mes, revenue acumulado y ranking
                dentro de cada categoría. Identifica los top 3 productos por
                revenue en cada categoría/mes e incluye alertas de tendencia
                accionables para detección temprana de caídas.

  Tablas      : dbo.[Order Details], dbo.Orders, dbo.Products, dbo.Categories
  Granularidad: 1 fila por (Mes × Categoría × Producto)
  Resultado   : Ordenado por Categoría → Producto → Año → Mes

  Columnas del resultado:
    Year                    — Año de la transacción
    Month                   — Mes de la transacción
    CategoryName            — Categoría del producto
    ProductName             — Nombre del producto
    MonthlyRevenue          — Revenue neto del mes (con descuentos aplicados)
    MonthlyOrders           — Órdenes únicas del mes
    PrevMonthRevenue        — Revenue del mes anterior (mismo producto)
    RevenueGrowthPct        — Crecimiento % vs mes anterior (NULL en primer mes)
    RunningRevenueByProduct — Revenue acumulado histórico del producto
    RevenueRankInCategory   — Ranking en la categoría ese mes (DENSE_RANK)
    Top3Flag                — 'TOP 3' si está entre los 3 mejores ese mes
    TrendAlert              — Alerta de tendencia basada en RevenueGrowthPct

  Correcciones v2.0 vs v1.0:
    1. LAG() separado en CTE propio → se calcula una sola vez, no duplicado
    2. ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW → frame explícito
       en el acumulado para comportamiento determinístico ante duplicados
    3. SELECT * reemplazado por columnas explícitas en todos los CTEs
    4. ROUND(..., 2) aplicado a métricas monetarias y porcentuales
    5. TrendAlert añadido para detectar caídas antes de que escalen

  Notas de rendimiento:
    · DATETRUNC mantiene tipo DATE en el CTE base → ORDER BY cronológico real
    · LAG en CTE separado evita recalcular la window function dos veces
    · INNER JOINs en capa base → descarte temprano de datos huérfanos
    · Frame explícito en SUM OVER → seguro ante OrderMonth duplicados
================================================================================
*/

WITH base_transactions AS (
    -- -------------------------------------------------------------------------
    -- CAPA 1: Granularidad Mes × Categoría × Producto
    --
    -- DATETRUNC(month) normaliza cualquier fecha del mes al día 1, garantizando
    -- agrupación mensual limpia y ORDER BY cronológico correcto en CTEs siguientes.
    --
    -- COUNT(DISTINCT OrderID): evita inflar el conteo por múltiples líneas de
    -- detalle dentro de una misma orden.
    --
    -- INNER JOINs en toda la cadena: descarte temprano de líneas huérfanas
    -- (sin orden, producto o categoría asociada). Reduce volumen upstream.
    -- -------------------------------------------------------------------------
    SELECT
        DATETRUNC(month, o.OrderDate)                        AS OrderMonth,
        c.CategoryName,
        p.ProductName,
        SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS MonthlyRevenue,
        COUNT(DISTINCT od.OrderID)                           AS MonthlyOrders
    FROM dbo.[Order Details] od
    INNER JOIN dbo.Orders     o ON od.OrderID   = o.OrderID
    INNER JOIN dbo.Products   p ON od.ProductID = p.ProductID
    INNER JOIN dbo.Categories c ON p.CategoryID = c.CategoryID
    GROUP BY
        DATETRUNC(month, o.OrderDate),
        c.CategoryName,
        p.ProductName
),

prev_month AS (
    -- -------------------------------------------------------------------------
    -- CAPA 2: Revenue del mes anterior por producto
    --
    -- LAG se separa en su propio CTE para calcularse UNA SOLA VEZ.
    -- Beneficio clave: el CTE siguiente puede referenciar PrevMonthRevenue
    -- directamente sin repetir la window function, eliminando el riesgo de
    -- inconsistencias si se modifica la lógica de partición en el futuro.
    -- -------------------------------------------------------------------------
    SELECT
        OrderMonth,
        CategoryName,
        ProductName,
        MonthlyRevenue,
        MonthlyOrders,
        LAG(MonthlyRevenue) OVER (
            PARTITION BY ProductName
            ORDER BY OrderMonth
        ) AS PrevMonthRevenue
    FROM base_transactions
),

enriched_metrics AS (
    -- -------------------------------------------------------------------------
    -- CAPA 3: Métricas analíticas (growth, acumulado, ranking)
    --
    -- RevenueGrowthPct:
    --   · Fórmula: (Actual - Anterior) / Anterior × 100
    --   · NULLIF(..., 0): evita división por cero si PrevMonthRevenue = 0
    --   · Retorna NULL en el primer mes del producto (sin mes anterior)
    --   · ROUND(..., 2): precisión adecuada para reportes ejecutivos
    --
    -- RunningRevenueByProduct:
    --   · ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW es el frame
    --     explícito. Sin esto, SQL Server usa RANGE por defecto, que incluye
    --     todos los peers del mismo OrderMonth en el frame → acumulado inflado
    --     si existen duplicados. Frame explícito = comportamiento determinístico.
    --
    -- RevenueRankInCategory:
    --   · DENSE_RANK sobre (Categoría × Mes): el ranking se reinicia cada mes
    --     y para cada categoría de forma independiente.
    --   · DENSE_RANK vs RANK: con empates produce 1,1,2 en lugar de 1,1,3.
    --     Correcto aquí porque no queremos "castigar" posiciones por empates.
    -- -------------------------------------------------------------------------
    SELECT
        OrderMonth,
        CategoryName,
        ProductName,
        MonthlyRevenue,
        MonthlyOrders,
        PrevMonthRevenue,

        ROUND(
            (MonthlyRevenue - PrevMonthRevenue)
            / NULLIF(PrevMonthRevenue, 0) * 100,
            2
        )                                                   AS RevenueGrowthPct,

        ROUND(
            SUM(MonthlyRevenue) OVER (
                PARTITION BY ProductName
                ORDER BY OrderMonth
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            2
        )                                                   AS RunningRevenueByProduct,

        DENSE_RANK() OVER (
            PARTITION BY CategoryName, OrderMonth
            ORDER BY MonthlyRevenue DESC
        )                                                   AS RevenueRankInCategory

    FROM prev_month
)

-- =============================================================================
-- SELECT FINAL: presentación del resultado para el Director de Ventas
--
-- Top3Flag    : etiqueta los 3 productos de mayor revenue por categoría y mes.
--               NULL en el resto facilita filtrado directo en Power BI o Excel.
--
-- TrendAlert  : convierte el análisis descriptivo en accionable.
--               Umbrales de negocio sugeridos (ajustables según industria):
--                 · Caída fuerte : < -20% respecto al mes anterior
--                 · En descenso  :   0% a -20%
--                 · En crecimiento: > +20%
--                 · Estable      : entre -20% y +20%
--               NULL cuando no hay mes anterior (primer mes del producto).
-- =============================================================================
SELECT
    YEAR(OrderMonth)                AS Year,
    MONTH(OrderMonth)               AS Month,
    CategoryName,
    ProductName,

    ROUND(MonthlyRevenue,    2)     AS MonthlyRevenue,
    MonthlyOrders,
    ROUND(PrevMonthRevenue,  2)     AS PrevMonthRevenue,
    RevenueGrowthPct,
    RunningRevenueByProduct,
    RevenueRankInCategory,

    CASE
        WHEN RevenueRankInCategory <= 3 THEN 'TOP 3'
        ELSE NULL
    END                             AS Top3Flag,

    CASE
        WHEN RevenueGrowthPct IS NULL  THEN NULL
        WHEN RevenueGrowthPct < -20    THEN 'CAÍDA FUERTE'
        WHEN RevenueGrowthPct <   0    THEN 'EN DESCENSO'
        WHEN RevenueGrowthPct >  20    THEN 'EN CRECIMIENTO'
        ELSE                                'ESTABLE'
    END                             AS TrendAlert

FROM enriched_metrics
ORDER BY
    CategoryName,
    ProductName,
    Year,
    Month;