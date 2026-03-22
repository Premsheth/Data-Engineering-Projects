CREATE DATABASE SCOPED CREDENTIAL cred_sp
WITH
    IDENTITY = 'MANAGED IDENTITY'

CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://azuresalesprojectsa.dfs.core.windows.net/silver',
    CREDENTIAL = cred_sp
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://azuresalesprojectsa.dfs.core.windows.net/gold',
    CREDENTIAL = cred_sp
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

------------------------------------------------
------- CREATE EXTERNAL TABLE EXTSALES
------------------------------------------------

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * from gold.sales

SELECT * from gold.extsales

