/*
================================================================================
  ANÁLISIS DE SALUD DEL NEGOCIO — Northwind Traders
  Versión     : 1.0
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

  Nota técnica:
    SQL Server devuelve múltiples resultsets en una sola ejecución cuando el
    script contiene más de un SELECT final. Cada herramienta los maneja distinto:
      · SSMS        → pestaña separada por resultset
      · Azure Data Studio → resultsets apilados
      · Python/JDBC → iterar con nextResultSet()
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
    -- garantizando una agrupación mensual limpia sin gaps por día.
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
    -- CAPA 2: Comparación con el mes anterior
    -- LAG(MonthlyRevenue, 1) recupera el revenue del período inmediatamente
    -- anterior dentro del orden cronológico. Retorna NULL para el primer mes
    -- del dataset, lo cual se propaga correctamente a RevenueGrowthPct.
    -- Se separa en su propio CTE para evitar anidar LAG dentro de cálculos
    -- aritméticos en la capa siguiente, mejorando legibilidad.
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
    --   NULLIF(..., 0) evita división por cero si el mes anterior tuvo revenue = 0.
    --   Retorna NULL en el primer mes (PrevMonthRevenue = NULL por LAG).
    --   ROUND(..., 2) limita a 2 decimales para legibilidad en reportes.
    --
    -- CumulativeRevenue:
    --   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW define la ventana
    --   desde la primera fila del dataset hasta la fila actual, produciendo
    --   un acumulado histórico completo mes a mes.
    --   Se especifica explícitamente aunque OrderMonth sea único por fila,
    --   siguiendo la misma práctica de ROWS sobre RANGE: hace la intención
    --   del código evidente para quien lo lea o mantenga en el futuro.
    --
    -- MovingAvg3Months:
    --   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW = ventana de 3 filas físicas:
    --   el mes actual + los 2 meses anteriores.
    --   Se elige ROWS sobre RANGE deliberadamente: RANGE agruparía filas con
    --   el mismo valor de ORDER BY, lo que podría distorsionar el promedio
    --   si existieran meses duplicados. ROWS opera sobre posición física,
    --   garantizando siempre exactamente 3 períodos en la ventana.
    --   Los primeros 2 meses del dataset promediarán con menos de 3 filas
    --   disponibles (comportamiento esperado y correcto de SQL).
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
        SUM(MonthlyRevenue) OVER (
            ORDER BY OrderMonth
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                     AS CumulativeRevenue,
        AVG(MonthlyRevenue) OVER (
            ORDER BY OrderMonth
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW         -- Ventana de 3 meses exactos
        )                                                     AS MovingAvg3Months
    FROM prev_month
)

SELECT
    YEAR(OrderMonth)  AS Year,
    MONTH(OrderMonth) AS Month,
    MonthlyRevenue,
    PrevMonthRevenue,
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
    -- Se consolidan todas las líneas de orden a nivel cliente en una sola pasada.
    -- INNER JOINs en cadena: descarta órdenes sin detalle y clientes sin registro
    -- en Customers (datos huérfanos que distorsionarían el cálculo del HHI).
    -- Revenue neto: UnitPrice * Quantity = bruto; * (1 - Discount) = con descuento.
    -- -------------------------------------------------------------------------
    SELECT
        o.CustomerID,
        c.CompanyName,
        SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS CustomerRevenue
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
    -- Se calcula una sola vez como CTE de 1 fila para evitar un subquery
    -- correlacionado que se ejecutaría N veces (una por cliente).
    -- Se usa en la capa siguiente via CROSS JOIN.
    -- -------------------------------------------------------------------------
    SELECT SUM(CustomerRevenue) AS TotalRevenue
    FROM customer_revenue
),

revenue_share AS (
    -- -------------------------------------------------------------------------
    -- CAPA 3: Participación porcentual de cada cliente sobre el total
    -- RevenueSharePct = CustomerRevenue / TotalRevenue × 100.
    -- NULLIF(..., 0) protege contra división por cero si TotalRevenue = 0,
    -- retornando NULL en lugar de un error en tiempo de ejecución.
    -- CROSS JOIN sobre CTE de 1 fila: trae TotalRevenue a cada fila de cliente
    -- sin costo adicional de procesamiento.
    -- -------------------------------------------------------------------------
    SELECT
        cr.CustomerID,
        cr.CompanyName,
        cr.CustomerRevenue,
        tr.TotalRevenue,
        cr.CustomerRevenue / NULLIF(tr.TotalRevenue, 0) * 100 AS RevenueSharePct
    FROM customer_revenue cr
    CROSS JOIN total_revenue tr
),

hhi_components AS (
    -- -------------------------------------------------------------------------
    -- CAPA 4: Componentes del índice HHI por cliente
    -- RevenueShareSq = (RevenueSharePct / 100)²
    --   Convierte el share a decimal antes de elevar al cuadrado, siguiendo
    --   la fórmula estándar del HHI: Σ(share_i²) donde share_i ∈ [0, 1].
    --
    -- HHI_Contribution = RevenueShareSq × 10,000
    --   Escala el resultado a la convención estándar del HHI (0–10,000).
    --   La suma de HHI_Contribution de todos los clientes produce el HHI total.
    --
    -- SQUARE() es equivalente a POWER(..., 2) pero más explícito semánticamente.
    -- -------------------------------------------------------------------------
    SELECT
        CustomerID,
        CompanyName,
        CustomerRevenue,
        TotalRevenue,
        RevenueSharePct,
        SQUARE(RevenueSharePct / 100)         AS RevenueShareSq,
        SQUARE(RevenueSharePct / 100) * 10000 AS HHI_Contribution
    FROM revenue_share
),

accumulated_share AS (
    -- -------------------------------------------------------------------------
    -- CAPA 5: Share porcentual acumulado (curva de concentración)
    -- Ordena clientes de mayor a menor RevenueSharePct y acumula el porcentaje.
    -- El último cliente mostrará AccumulatedSharePct ≈ 100%.
    -- Útil para responder: "¿Cuántos clientes concentran el X% del revenue?"
    --
    -- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: ventana explícita para
    -- garantizar acumulado por posición física, consistente con la práctica
    -- aplicada en las demás queries del análisis.
    -- -------------------------------------------------------------------------
    SELECT
        *,
        SUM(RevenueSharePct) OVER (
            ORDER BY RevenueSharePct DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS AccumulatedSharePct
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
    -- Segmentación de clientes por volumen de revenue (NTILE en deciles).
    -- NTILE(10) divide los clientes en 10 grupos iguales ordenados de mayor
    -- a menor revenue. La lógica de agrupación es:
    --   Decil 1          → TOP 10%    (el 10% de mayor revenue)
    --   Deciles 2 al 5   → MIDDLE 40% (el siguiente 40%)
    --   Deciles 6 al 10  → BOTTOM 50% (el 50% restante)
    --
    -- Nota: NTILE se evalúa dos veces porque SQL Server no permite referenciar
    -- alias de window functions dentro del mismo SELECT. Si se requiere evitar
    -- la doble evaluación, envolver accumulated_share en un CTE adicional.
    -- -------------------------------------------------------------------------
    CASE
        WHEN NTILE(10) OVER (ORDER BY CustomerRevenue DESC) = 1  THEN 'TOP 10%'
        WHEN NTILE(10) OVER (ORDER BY CustomerRevenue DESC) <= 5 THEN 'MIDDLE 40%'
        ELSE 'BOTTOM 50%'
    END AS CustomerSegment
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
    -- MAX(OrderDate) se usa como "hoy histórico" en lugar de GETDATE() para
    -- garantizar resultados reproducibles independientemente de cuándo se
    -- ejecute la query. Crítico para auditorías y comparaciones históricas.
    -- CTE de 1 fila: se trae a la siguiente capa via CROSS JOIN sin costo.
    -- -------------------------------------------------------------------------
    SELECT MAX(OrderDate) AS ReferenceDate
    FROM dbo.Orders
),

customer_activity AS (
    -- -------------------------------------------------------------------------
    -- CAPA 2: Métricas de actividad y revenue por cliente
    -- Consolida en una sola pasada: fechas de primera/última compra y revenue
    -- histórico total, evitando múltiples escaneos sobre las mismas tablas.
    --
    -- INNER JOINs en cadena: descarta líneas de detalle sin orden asociada
    -- y clientes sin registro en Customers (datos huérfanos).
    --
    -- CROSS JOIN reference_date: trae ReferenceDate a cada fila de cliente
    -- para calcular DaysSinceLastPurchase en la capa siguiente sin subquery.
    --
    -- Revenue neto: UnitPrice * Quantity = bruto; * (1 - Discount) = con descuento.
    -- -------------------------------------------------------------------------
    SELECT
        o.CustomerID,
        c.CompanyName,
        c.Country,
        rd.ReferenceDate,
        MIN(o.OrderDate)                                     AS FirstPurchaseDate,
        MAX(o.OrderDate)                                     AS LastPurchaseDate,
        SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalHistoricRevenue
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
    -- CAPA 3: Cálculo de inactividad, revenue promedio y clasificación de churn
    --
    -- DaysSinceLastPurchase:
    --   DATEDIFF(DAY, Last, Reference) = días transcurridos desde la última
    --   compra hasta el fin del dataset. A mayor valor → cliente más inactivo.
    --
    -- AvgMonthlyRevenue:
    --   TotalHistoricRevenue / DATEDIFF(MONTH, First, Last).
    --   TRAMPA IMPORTANTE: si First = Last (cliente de compra única),
    --   DATEDIFF = 0 → NULLIF devuelve NULL en lugar de producir error de
    --   división por cero. El NULL indica que el promedio no es calculable,
    --   no que el revenue sea cero. Documentar este caso en reportes al negocio.
    --
    -- ChurnRisk:
    --   Clasificación en 3 segmentos basada en DaysSinceLastPurchase.
    --   Los umbrales (180 / 90 días) son parámetros de negocio — si cambian,
    --   solo requieren modificación en este CTE sin afectar capas anteriores.
    -- -------------------------------------------------------------------------
    SELECT
        CustomerID,
        CompanyName,
        Country,
        FirstPurchaseDate,
        LastPurchaseDate,
        DATEDIFF(DAY, LastPurchaseDate, ReferenceDate)       AS DaysSinceLastPurchase,
        TotalHistoricRevenue,
        TotalHistoricRevenue
            / NULLIF(DATEDIFF(MONTH, FirstPurchaseDate, LastPurchaseDate), 0)
                                                             AS AvgMonthlyRevenue,
        CASE
            WHEN DATEDIFF(DAY, LastPurchaseDate, ReferenceDate) >= 180 THEN 'LOST'
            WHEN DATEDIFF(DAY, LastPurchaseDate, ReferenceDate) >= 90  THEN 'AT RISK'
            ELSE 'ACTIVE'
        END                                                  AS ChurnRisk
    FROM customer_activity
)

SELECT
    CustomerID,
    CompanyName,
    Country,
    LastPurchaseDate,
    DaysSinceLastPurchase,
    TotalHistoricRevenue,
    AvgMonthlyRevenue,
    ChurnRisk
FROM customer_recency
ORDER BY DaysSinceLastPurchase DESC;
