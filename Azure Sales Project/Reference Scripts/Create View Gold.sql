--------------------------
-- Create View Calender
--------------------------

CREATE VIEW gold.calender
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://azuresalesprojectsa.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
                FORMAT = 'PARQUET'

    ) as cal_view

--------------------------
-- Create View Customer
--------------------------

CREATE VIEW gold.customer
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://azuresalesprojectsa.dfs.core.windows.net/silver/AdventureWorks_Customers/',
                FORMAT = 'PARQUET'

    ) as cust_view

--------------------------
-- Create View Products
--------------------------

CREATE VIEW gold.products
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://azuresalesprojectsa.dfs.core.windows.net/silver/AdventureWorks_Products/',
                FORMAT = 'PARQUET'

    ) as prod_view

--------------------------
-- Create View Returns
--------------------------

CREATE VIEW gold.returns
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://azuresalesprojectsa.dfs.core.windows.net/silver/AdventureWorks_Returns/',
                FORMAT = 'PARQUET'

    ) as retu_view

--------------------------
-- Create View Sales
--------------------------

CREATE VIEW gold.sales
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://azuresalesprojectsa.dfs.core.windows.net/silver/AdventureWorks_Sales/',
                FORMAT = 'PARQUET'

    ) as sale_view

--------------------------
-- Create View Territories
--------------------------
CREATE VIEW gold.territories
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://azuresalesprojectsa.dfs.core.windows.net/silver/AdventureWorks_Territories/',
                FORMAT = 'PARQUET'

    ) as terr_view

--------------------------
-- Create View Product Category
--------------------------

CREATE VIEW gold.category
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://azuresalesprojectsa.dfs.core.windows.net/silver/Product_SubCategories/',
                FORMAT = 'PARQUET'

    ) as prodcat_view

