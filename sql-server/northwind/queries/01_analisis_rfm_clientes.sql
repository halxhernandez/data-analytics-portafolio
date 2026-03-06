/*
================================================================================
  ANÁLISIS RFM DE CLIENTES — Northwind Traders
  Versión     : 2.0 (production-ready)
  Autor       : [Tu nombre]
  Fecha       : 2026-03-05
================================================================================
  Descripción : Calcula métricas clave de comportamiento de compra por cliente:
                Recency, Frequency y Monetary (RFM), incluyendo ranking de
                revenue dentro del país y participación porcentual.

  Tablas      : dbo.[Order Details], Orders, Customers
  Filtro      : Solo clientes con TotalOrders >= 2
  Resultado   : 1 fila por cliente activo, ordenado por País y RevenueRank

  Columnas del resultado:
    CustomerID              — Identificador del cliente
    CompanyName             — Nombre de la empresa
    Country                 — País del cliente
    TotalOrders             — Número de órdenes únicas (Frequency)
    TotalRevenue            — Revenue neto total con descuentos (Monetary)
    AvgOrderValue           — Promedio de revenue por orden
    FirstPurchase           — Fecha de la primera compra
    LastPurchase            — Fecha de la última compra (Recency base)
    DaysSinceLastPurchase   — Días de inactividad desde LastPurchase (Recency)
    RevenueRank             — Ranking dentro del país (1 = mayor revenue)
    CountryTotalRevenue     — Revenue total del país del cliente
    PctOfCountryRevenue     — % que representa el cliente sobre su país

  Notas de rendimiento:
    · max_date como CTE evita un subquery correlacionado (1 ejecución vs N filas)
    · HAVING en customer_stats filtra antes de los JOINs (reduce filas upstream)
    · CROSS JOIN sobre CTE de 1 fila es equivalente a un escalar, sin costo
================================================================================
*/

WITH revenue_by_order AS (
    -- -------------------------------------------------------------------------
    -- CAPA 1: Revenue neto por orden
    -- Agrupa a nivel OrderID para desacoplar el cálculo de línea del de cliente.
    -- UnitPrice * Quantity = bruto; * (1 - Discount) = neto con descuento.
    --
    -- Buena práctica: siempre aplicar descuentos en esta capa, no más arriba,
    -- para evitar errores de agregación doble en CTEs posteriores.
    -- -------------------------------------------------------------------------
    SELECT
        OrderID,
        SUM(UnitPrice * Quantity * (1 - Discount)) AS OrderRevenue
    FROM dbo.[Order Details]
    GROUP BY OrderID
),

customer_stats AS (
    -- -------------------------------------------------------------------------
    -- CAPA 2: Métricas agregadas por cliente (Frequency + Monetary base)
    --
    -- LEFT JOIN: conserva órdenes sin detalle en [Order Details] (datos sucios).
    --   COALESCE garantiza revenue = 0 en esos casos en lugar de NULL.
    --
    -- HAVING >= 2: filtra clientes de compra única ANTES de los JOINs finales.
    --   Más eficiente que filtrar en el WHERE del SELECT principal porque
    --   reduce el volumen de filas que llegan a los pasos siguientes.
    --
    -- AVG(OrderRevenue): promedio real de valores de orden individuales.
    --   Equivalente a SUM/COUNT con datos limpios, pero semánticamente más
    --   preciso y robusto ante valores NULL en revenue_by_order.
    -- -------------------------------------------------------------------------
    SELECT
        o.CustomerID,
        COUNT(DISTINCT o.OrderID)       AS TotalOrders,
        SUM(COALESCE(r.OrderRevenue, 0)) AS TotalRevenue,
        AVG(COALESCE(r.OrderRevenue, 0)) AS AvgOrderValue,
        MIN(o.OrderDate)                AS FirstPurchase,
        MAX(o.OrderDate)                AS LastPurchase
    FROM Orders o
    LEFT JOIN revenue_by_order r ON o.OrderID = r.OrderID
    GROUP BY o.CustomerID
    HAVING COUNT(DISTINCT o.OrderID) >= 2
),

max_date AS (
    -- -------------------------------------------------------------------------
    -- CAPA 3: Fecha de referencia del dataset
    -- Se usa MAX(OrderDate) como "hoy histórico" en lugar de GETDATE() para
    -- garantizar resultados reproducibles independientemente de cuándo se
    -- ejecute la query. Crítico para auditorías y comparaciones entre períodos.
    -- -------------------------------------------------------------------------
    SELECT MAX(OrderDate) AS MaxDate
    FROM Orders
)

SELECT
    cs.CustomerID,
    cu.CompanyName,
    cu.Country,

    -- ── FREQUENCY ────────────────────────────────────────────────────────────
    cs.TotalOrders,

    -- ── MONETARY ─────────────────────────────────────────────────────────────
    ROUND(cs.TotalRevenue,  2)                              AS TotalRevenue,
    ROUND(cs.AvgOrderValue, 2)                              AS AvgOrderValue,

    -- ── RECENCY ──────────────────────────────────────────────────────────────
    cs.FirstPurchase,
    cs.LastPurchase,
    DATEDIFF(DAY, cs.LastPurchase, md.MaxDate)              AS DaysSinceLastPurchase,

    -- ── RANKING POR PAÍS ─────────────────────────────────────────────────────
    -- RANK() permite empates: dos clientes con igual revenue comparten posición
    -- y el siguiente número se salta (1, 1, 3...).
    -- Usar DENSE_RANK() si se prefiere secuencia sin saltos (1, 1, 2...).
    -- ORDER BY TotalRevenue DESC (campo base) en lugar del alias RevenueRank
    -- para mayor claridad y compatibilidad entre engines SQL.
    RANK() OVER (
        PARTITION BY cu.Country
        ORDER BY cs.TotalRevenue DESC
    )                                                       AS RevenueRank,

    -- ── CONTEXTO DE PAÍS (métricas adicionales anticipando próximas preguntas)
    -- Permite al Gerente Comercial ver concentración de revenue por país
    -- sin necesidad de una segunda query.
    ROUND(
        SUM(cs.TotalRevenue) OVER (PARTITION BY cu.Country),
        2
    )                                                       AS CountryTotalRevenue,

    ROUND(
        cs.TotalRevenue * 100.0
        / NULLIF(SUM(cs.TotalRevenue) OVER (PARTITION BY cu.Country), 0),
        2
    )                                                       AS PctOfCountryRevenue

FROM customer_stats cs

-- INNER JOIN: descarta clientes huérfanos (en Orders pero sin registro en Customers).
-- Decisión de diseño: si se necesita auditar huérfanos, cambiar a LEFT JOIN
-- y agregar: WHERE cu.CustomerID IS NULL en una query separada.
INNER JOIN Customers cu
    ON cs.CustomerID = cu.CustomerID

-- CROSS JOIN sobre CTE de 1 fila: equivale a referenciar un escalar.
-- Evita repetir el subquery MAX(OrderDate) en cada fila del SELECT.
CROSS JOIN max_date md

-- Orden final por País y posición de revenue (campo base, no alias).
-- Más explícito y portable entre SQL Server, PostgreSQL y otros engines.
ORDER BY
    cu.Country      ASC,
    cs.TotalRevenue DESC;
