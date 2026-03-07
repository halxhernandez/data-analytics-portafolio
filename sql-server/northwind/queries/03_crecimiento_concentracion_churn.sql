/*
================================================================================
  ANÁLISIS DE SALUD DEL NEGOCIO — Northwind Traders
  Versión     : 2.0 (production-ready)
  Autor       : Alejandro Hernández Rodríguez
  Fecha       : 2026-03-05
================================================================================
  Descripción : Consolida los tres análisis ejecutivos del Reto #3 en un único
                script. Cada análisis se ejecuta de forma independiente mediante
                sus propios CTEs y produce su propio conjunto de resultados.

  Análisis incluidos:
    [1] Crecimiento del negocio mes a mes
    [2] Concentración de clientes (índice HHI simplificado)
    [3] Detección de clientes perdidos (Churn)

  Tablas      : dbo.[Order Details], dbo.Orders, dbo.Customers
  Ejecución   : Seleccionar y ejecutar cada bloque por separado (ver separadores)
                o ejecutar el script completo para obtener los 3 resultsets.

  Correcciones v2.0 vs v1.0:
    1. AvgMonthlyRevenue: DATEDIFF(MONTH) reemplazado por DATEDIFF(DAY)/30.0
       → meses reales transcurridos, no cambios de mes calendario
    2. ROUND(..., 2) aplicado a todas las métricas monetarias en Q2 y Q3
    3. SELECT * en accumulated_share (Q2) reemplazado por columnas explícitas
    4. FirstPurchaseDate añadido al SELECT final de Q3
    5. Consistencia de estilo ROWS BETWEEN en todos los acumulados

  Nota técnica — múltiples resultsets:
    SQL Server devuelve múltiples resultsets en una sola ejecución cuando el
    script contiene más de un SELECT final. Cada herramienta los maneja distinto:
      · SSMS             → pestaña separada por resultset
      · Azure Data Studio → resultsets apilados
      · Python/JDBC      → iterar con nextResultSet()
================================================================================
*/


-- ============================================================================
-- [1] CRECIMIENTO DEL NEGOCIO MES A MES
-- ============================================================================
-- Granularidad : 1 fila por mes
-- Orden        : Cronológico (Year ASC, Month ASC)
-- ============================================================================

WITH monthly_revenue AS (
    -- -------------------------------------------------------------------------
    -- CAPA 1: Revenue neto agregado por mes
    -- DATETRUNC(MONTH) normaliza todas las fechas del mes al día 1,
    -- garantizando agrupación mensual limpia sin gaps por día.
    -- INNER JOIN descarta líneas de detalle sin orden asociada (datos huérfanos).
    -- Revenue neto: UnitPrice * Quantity = bruto; * (1 - Discount) = con descuento.
    -- -------------------------------------------------------------------------
    SELECT
        DATETRUNC(MONTH, o.OrderDate)              AS OrderMonth,
        SUM(UnitPrice * Quantity * (1 - Discount)) AS MonthlyRevenue
    FROM dbo.[Order Details] od
    INNER JOIN dbo.Orders o ON od.OrderID = o.OrderID
    GROUP BY DATETRUNC(MONTH, o.OrderDate)
),

prev_month AS (
    -- -------------------------------------------------------------------------
    -- CAPA 2: Revenue del mes anterior (LAG separado en CTE propio)
    -- Se calcula una sola vez para evitar repetir la window function dentro
    -- de expresiones aritméticas en la capa siguiente.
    -- Retorna NULL para el primer mes → se propaga correctamente a GrowthPct.
    -- -------------------------------------------------------------------------
    SELECT
        OrderMonth,
        MonthlyRevenue,
        LAG(MonthlyRevenue) OVER (ORDER BY OrderMonth) AS PrevMonthRevenue
    FROM monthly_revenue
),

enriched_metrics AS (
    -- -------------------------------------------------------------------------
    -- CAPA 3: Métricas analíticas con window functions
    --
    -- RevenueGrowthPct:
    --   Fórmula: (Actual - Anterior) / Anterior × 100.
    --   NULLIF(..., 0) evita división por cero si el mes anterior fue 0.
    --   NULL en el primer mes (sin mes anterior). ROUND a 2 decimales.
    --
    -- CumulativeRevenue:
    --   Frame explícito ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    --   Aunque OrderMonth es único por fila, el frame explícito hace la
    --   intención del código evidente y es robusto ante cambios de datos.
    --
    -- MovingAvg3Months:
    --   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW = 3 filas físicas exactas.
    --   ROWS sobre RANGE: opera por posición física, no por valor de ORDER BY.
    --   Garantiza exactamente 3 períodos incluso ante OrderMonth duplicados.
    --   Primeros 2 meses promedian con menos filas (comportamiento correcto).
    -- -------------------------------------------------------------------------
    SELECT
        OrderMonth,
        MonthlyRevenue,
        PrevMonthRevenue,
        ROUND(
            (MonthlyRevenue - PrevMonthRevenue)
            / NULLIF(PrevMonthRevenue, 0) * 100,
            2
        )                                                     AS RevenueGrowthPct,
        ROUND(
            SUM(MonthlyRevenue) OVER (
                ORDER BY OrderMonth
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            2
        )                                                     AS CumulativeRevenue,
        ROUND(
            AVG(MonthlyRevenue) OVER (
                ORDER BY OrderMonth
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ),
            2
        )                                                     AS MovingAvg3Months
    FROM prev_month
)

SELECT
    YEAR(OrderMonth)  AS Year,
    MONTH(OrderMonth) AS Month,
    ROUND(MonthlyRevenue,     2) AS MonthlyRevenue,
    ROUND(PrevMonthRevenue,   2) AS PrevMonthRevenue,
    RevenueGrowthPct,
    CumulativeRevenue,
    MovingAvg3Months
FROM enriched_metrics
ORDER BY Year, Month;


-- ============================================================================
-- [2] CONCENTRACIÓN DE CLIENTES — Índice HHI Simplificado
-- ============================================================================
-- Granularidad : 1 fila por cliente
-- Orden        : RevenueSharePct DESC (mayor concentración primero)
-- HHI total    : SELECT SUM(HHI_Contribution) FROM este resultset
-- Referencia   : HHI < 1500 = saludable | 1500-2500 = moderado | > 2500 = crítico
-- ============================================================================

WITH customer_revenue AS (
    -- -------------------------------------------------------------------------
    -- CAPA 1: Revenue neto agregado por cliente
    -- Consolida todas las líneas de orden a nivel cliente en una sola pasada.
    -- INNER JOINs: descarta órdenes sin detalle y clientes huérfanos.
    -- Revenue neto: UnitPrice * Quantity = bruto; * (1 - Discount) = con descuento.
    -- -------------------------------------------------------------------------
    SELECT
        o.CustomerID,
        c.CompanyName,
        ROUND(
            SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)),
            2
        )                                                    AS CustomerRevenue
    FROM dbo.[Order Details] od
    INNER JOIN dbo.Orders    o ON od.OrderID   = o.OrderID
    INNER JOIN dbo.Customers c ON o.CustomerID = c.CustomerID
    GROUP BY
        o.CustomerID,
        c.CompanyName
),

total_revenue AS (
    -- -------------------------------------------------------------------------
    -- CAPA 2: Revenue total de la empresa (escalar)
    -- CTE de 1 fila para evitar subquery correlacionada que ejecutaría N veces.
    -- Se incorpora a la siguiente capa via CROSS JOIN sin costo adicional.
    -- -------------------------------------------------------------------------
    SELECT ROUND(SUM(CustomerRevenue), 2) AS TotalRevenue
    FROM customer_revenue
),

revenue_share AS (
    -- -------------------------------------------------------------------------
    -- CAPA 3: Participación porcentual de cada cliente sobre el total
    -- RevenueSharePct = CustomerRevenue / TotalRevenue × 100.
    -- NULLIF(..., 0) protege contra división por cero si TotalRevenue = 0.
    -- CROSS JOIN sobre CTE de 1 fila: trae TotalRevenue sin subquery correlacionada.
    -- -------------------------------------------------------------------------
    SELECT
        cr.CustomerID,
        cr.CompanyName,
        cr.CustomerRevenue,
        tr.TotalRevenue,
        ROUND(
            cr.CustomerRevenue / NULLIF(tr.TotalRevenue, 0) * 100,
            4
        )                                                    AS RevenueSharePct
    FROM customer_revenue cr
    CROSS JOIN total_revenue tr
),

hhi_components AS (
    -- -------------------------------------------------------------------------
    -- CAPA 4: Componentes del índice HHI por cliente
    -- RevenueShareSq    = (RevenueSharePct / 100)²
    --   Convierte share a decimal antes de elevar al cuadrado.
    --   Fórmula estándar HHI: Σ(share_i²) donde share_i ∈ [0, 1].
    -- HHI_Contribution  = RevenueShareSq × 10,000
    --   Escala a convención estándar HHI (rango 0–10,000).
    --   Suma de HHI_Contribution de todos los clientes = HHI total de la empresa.
    -- SQUARE() equivale a POWER(..., 2), más explícito semánticamente.
    -- -------------------------------------------------------------------------
    SELECT
        CustomerID,
        CompanyName,
        CustomerRevenue,
        TotalRevenue,
        RevenueSharePct,
        ROUND(SQUARE(RevenueSharePct / 100),          6) AS RevenueShareSq,
        ROUND(SQUARE(RevenueSharePct / 100) * 10000,  4) AS HHI_Contribution
    FROM revenue_share
),

accumulated_share AS (
    -- -------------------------------------------------------------------------
    -- CAPA 5: Share porcentual acumulado (curva de concentración de Pareto)
    -- Ordena de mayor a menor RevenueSharePct y acumula el porcentaje.
    -- Útil para responder: "¿cuántos clientes concentran el X% del revenue?"
    -- Columnas explícitas (no SELECT *): robusto ante cambios en CTEs upstream.
    -- ROWS BETWEEN explícito: consistente con el estándar del script completo.
    -- -------------------------------------------------------------------------
    SELECT
        CustomerID,
        CompanyName,
        CustomerRevenue,
        TotalRevenue,
        RevenueSharePct,
        RevenueShareSq,
        HHI_Contribution,
        ROUND(
            SUM(RevenueSharePct) OVER (
                ORDER BY RevenueSharePct DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            2
        )                                                    AS AccumulatedSharePct
    FROM hhi_components
)

SELECT
    CustomerID,
    CompanyName,
    CustomerRevenue,
    TotalRevenue,
    RevenueSharePct,
    RevenueShareSq,
    HHI_Contribution,
    AccumulatedSharePct,
    -- -------------------------------------------------------------------------
    -- Segmentación de clientes por deciles de revenue (NTILE).
    -- NTILE(10) divide clientes en 10 grupos iguales de mayor a menor revenue.
    --   Decil 1        → TOP 10%    (10% de mayor revenue)
    --   Deciles 2 a 5  → MIDDLE 40% (siguiente 40%)
    --   Deciles 6 a 10 → BOTTOM 50% (50% restante)
    -- NTILE se evalúa dos veces porque SQL Server no permite referenciar alias
    -- de window functions dentro del mismo SELECT. Para evitar doble evaluación
    -- en datasets grandes, envolver accumulated_share en un CTE adicional.
    -- -------------------------------------------------------------------------
    CASE
        WHEN NTILE(10) OVER (ORDER BY CustomerRevenue DESC) = 1  THEN 'TOP 10%'
        WHEN NTILE(10) OVER (ORDER BY CustomerRevenue DESC) <= 5 THEN 'MIDDLE 40%'
        ELSE 'BOTTOM 50%'
    END                                                      AS CustomerSegment
FROM accumulated_share
ORDER BY RevenueSharePct DESC;


-- ============================================================================
-- [3] DETECCIÓN DE CLIENTES PERDIDOS (CHURN)
-- ============================================================================
-- Granularidad : 1 fila por cliente
-- Orden        : DaysSinceLastPurchase DESC (mayor inactividad primero)
-- Umbrales     : LOST >= 180 días | AT RISK 90-179 días | ACTIVE <= 89 días
-- ============================================================================

WITH reference_date AS (
    -- -------------------------------------------------------------------------
    -- CAPA 1: Fecha de referencia del dataset
    -- MAX(OrderDate) como "hoy histórico" en lugar de GETDATE() garantiza
    -- reproducibilidad independientemente de cuándo se ejecute la query.
    -- Crítico para auditorías y comparaciones entre períodos históricos.
    -- CTE de 1 fila: se incorpora via CROSS JOIN sin costo de procesamiento.
    -- -------------------------------------------------------------------------
    SELECT MAX(OrderDate) AS ReferenceDate
    FROM dbo.Orders
),

customer_activity AS (
    -- -------------------------------------------------------------------------
    -- CAPA 2: Métricas de actividad y revenue por cliente
    -- Consolida en una sola pasada: fechas de primera/última compra y revenue.
    -- INNER JOINs: descarta líneas huérfanas sin orden o sin registro en Customers.
    -- CROSS JOIN reference_date: trae ReferenceDate a cada fila sin subquery.
    -- Revenue neto: UnitPrice * Quantity = bruto; * (1 - Discount) = con descuento.
    -- -------------------------------------------------------------------------
    SELECT
        o.CustomerID,
        c.CompanyName,
        c.Country,
        rd.ReferenceDate,
        MIN(o.OrderDate)                                      AS FirstPurchaseDate,
        MAX(o.OrderDate)                                      AS LastPurchaseDate,
        ROUND(
            SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)),
            2
        )                                                     AS TotalHistoricRevenue
    FROM dbo.[Order Details] od
    INNER JOIN dbo.Orders    o  ON od.OrderID   = o.OrderID
    INNER JOIN dbo.Customers c  ON o.CustomerID = c.CustomerID
    CROSS JOIN reference_date rd
    GROUP BY
        o.CustomerID,
        c.CompanyName,
        c.Country,
        rd.ReferenceDate
),

customer_recency AS (
    -- -------------------------------------------------------------------------
    -- CAPA 3: Inactividad, revenue promedio mensual y clasificación de churn
    --
    -- DaysSinceLastPurchase:
    --   DATEDIFF(DAY, Last, Reference) = días de inactividad hasta fin del dataset.
    --   A mayor valor → cliente más inactivo.
    --
    -- AvgMonthlyRevenue:
    --   TotalHistoricRevenue / (DATEDIFF(DAY, First, Last) / 30.0)
    --   CORRECCIÓN v2.0: se reemplaza DATEDIFF(MONTH, ...) por DATEDIFF(DAY)/30.0
    --   porque DATEDIFF(MONTH) cuenta cambios de mes calendario, no meses reales.
    --   Ejemplo del problema: DATEDIFF(MONTH, '2024-01-31', '2024-02-01') = 1
    --   aunque solo transcurrió 1 día. Con DAY/30.0 el resultado es 0.03 meses,
    --   mucho más representativo del período real de actividad.
    --   NULLIF(..., 0): cliente con FirstPurchaseDate = LastPurchaseDate
    --   (compra única) → denominador = 0 → NULL en lugar de error de ejecución.
    --   Interpretar NULL como "cliente de compra única, promedio no aplicable".
    --
    -- ChurnRisk:
    --   Umbrales (180/90 días) son parámetros de negocio definidos en este CTE.
    --   Si cambian, solo requieren modificación aquí sin afectar capas anteriores.
    -- -------------------------------------------------------------------------
    SELECT
        CustomerID,
        CompanyName,
        Country,
        FirstPurchaseDate,
        LastPurchaseDate,
        DATEDIFF(DAY, LastPurchaseDate, ReferenceDate)        AS DaysSinceLastPurchase,
        TotalHistoricRevenue,
        ROUND(
            TotalHistoricRevenue
                / NULLIF(DATEDIFF(DAY, FirstPurchaseDate, LastPurchaseDate) / 30.0, 0),
            2
        )                                                     AS AvgMonthlyRevenue,
        CASE
            WHEN DATEDIFF(DAY, LastPurchaseDate, ReferenceDate) >= 180 THEN 'LOST'
            WHEN DATEDIFF(DAY, LastPurchaseDate, ReferenceDate) >= 90  THEN 'AT RISK'
            ELSE 'ACTIVE'
        END                                                   AS ChurnRisk
    FROM customer_activity
)

SELECT
    CustomerID,
    CompanyName,
    Country,
    FirstPurchaseDate,        -- v2.0: añadido para contexto completo de la relación
    LastPurchaseDate,
    DaysSinceLastPurchase,
    TotalHistoricRevenue,
    AvgMonthlyRevenue,
    ChurnRisk
FROM customer_recency
ORDER BY DaysSinceLastPurchase DESC;
