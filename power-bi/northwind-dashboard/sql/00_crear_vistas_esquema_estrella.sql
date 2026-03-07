/*
================================================================================
  VISTAS DEL MODELO ESTRELLA — Northwind Traders
  Versión     : 1.0
  Autor       : Alejandro Hernández Rodríguez
  Fecha       : 2026-03-05
================================================================================
  Descripción : Crea las vistas que conforman el modelo estrella para el
                dashboard ejecutivo en Power BI (Reto #4). Cada vista
                corresponde a una tabla del modelo: 1 tabla de hechos y
                3 dimensiones.

  Vistas creadas:
    dbo.vw_FactSales      — Tabla de hechos: transacciones de venta
    dbo.vw_DimCustomer    — Dimensión cliente con normalización de país
    dbo.vw_DimProduct     — Dimensión producto
    dbo.vw_DimCategory    — Dimensión categoría

  Tablas fuente : dbo.[Order Details], dbo.Orders, dbo.Products,
                  dbo.Categories, dbo.Customers
  Destino       : Power BI Desktop (importación directa desde SQL Server)

  Notas de diseño:
    · Patrón IF OBJECT_ID + DROP + GO + CREATE en cada vista garantiza
      idempotencia: el script puede ejecutarse múltiples veces sin error,
      recreando siempre las vistas con la definición más reciente.
    · GO es obligatorio entre DROP y CREATE VIEW porque SQL Server requiere
      que CREATE VIEW sea el primer statement de su batch.
    · Las vistas no reemplazan el modelo de datos en Power BI — son una
      capa de preparación que simplifica la importación y centraliza
      la lógica de negocio en SQL Server.
================================================================================
*/


-- ============================================================================
-- [1] vw_FactSales — Tabla de hechos
-- ============================================================================
-- Expone una fila por línea de orden con el revenue neto calculado.
-- Es el centro del modelo estrella: se relaciona con todas las dimensiones.
-- ============================================================================

IF OBJECT_ID('dbo.vw_FactSales', 'V') IS NOT NULL
    DROP VIEW dbo.vw_FactSales
GO

CREATE VIEW dbo.vw_FactSales AS
SELECT
    od.OrderID,
    o.CustomerID,
    od.ProductID,
    p.CategoryID,
    o.OrderDate,
    od.Quantity,
    od.UnitPrice,
    od.Discount,
    -- Revenue neto por línea: UnitPrice * Quantity = bruto; * (1 - Discount) = con descuento.
    -- Se calcula aquí para evitar repetir la fórmula en cada medida DAX de Power BI.
    od.Quantity * od.UnitPrice * (1 - od.Discount) AS NetRevenue
FROM dbo.[Order Details] od
-- INNER JOIN: descarta líneas sin orden o sin producto asociado (datos huérfanos).
INNER JOIN dbo.Orders     o ON od.OrderID   = o.OrderID
INNER JOIN dbo.Products   p ON od.ProductID = p.ProductID
-- LEFT JOIN: conserva líneas cuyo producto no tenga categoría asignada.
-- CategoryID ya viene de Products, por lo que este JOIN es solo de referencia.
LEFT JOIN  dbo.Categories c ON p.CategoryID = c.CategoryID
GO


-- ============================================================================
-- [2] vw_DimCustomer — Dimensión cliente
-- ============================================================================
-- Expone los atributos descriptivos del cliente para filtros y agrupaciones
-- en el dashboard. Incluye normalización de nombres de país inconsistentes
-- en el dataset original de Northwind.
-- ============================================================================

-- Nota: el segundo parámetro de OBJECT_ID es 'U' (tabla) en lugar de 'V'
-- (vista) en las dimensiones del script original — debería ser 'V' en todos
-- los casos. Se mantiene el patrón para no alterar el comportamiento,
-- pero se recomienda corregir a 'V' para consistencia.
IF OBJECT_ID('dbo.vw_DimCustomer', 'V') IS NOT NULL
    DROP VIEW dbo.vw_DimCustomer
GO

CREATE VIEW dbo.vw_DimCustomer AS
SELECT
    CustomerID,
    CompanyName,
    -- Normalización de país: Northwind usa abreviaciones inconsistentes
    -- ('USA', 'UK') mezcladas con nombres completos. TRIM elimina espacios
    -- en blanco que podrían romper filtros en Power BI.
    CASE
        WHEN TRIM(Country) = 'USA' THEN 'United States'
        WHEN TRIM(Country) = 'UK'  THEN 'United Kingdom'
        ELSE TRIM(Country)
    END AS Country,
    City
FROM dbo.Customers
GO


-- ============================================================================
-- [3] vw_DimProduct — Dimensión producto
-- ============================================================================
-- Expone los atributos del producto necesarios para el modelo.
-- CategoryID actúa como FK hacia vw_DimCategory en el modelo estrella.
-- ============================================================================

IF OBJECT_ID('dbo.vw_DimProduct', 'V') IS NOT NULL
    DROP VIEW dbo.vw_DimProduct
GO

CREATE VIEW dbo.vw_DimProduct AS
SELECT
    ProductID,
    ProductName,
    CategoryID  -- FK hacia vw_DimCategory
FROM dbo.Products
GO


-- ============================================================================
-- [4] vw_DimCategory — Dimensión categoría
-- ============================================================================
-- Expone los atributos de categoría para agrupaciones y filtros en el dashboard.
-- Se relaciona con vw_FactSales y vw_DimProduct mediante CategoryID.
-- ============================================================================

IF OBJECT_ID('dbo.vw_DimCategory', 'V') IS NOT NULL
    DROP VIEW dbo.vw_DimCategory
GO

CREATE VIEW dbo.vw_DimCategory AS
SELECT
    CategoryID,
    CategoryName
FROM dbo.Categories
GO
