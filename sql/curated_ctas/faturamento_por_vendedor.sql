CREATE TABLE lakehouse_curated.faturamento_por_vendedor 
WITH ( format = 'PARQUET', parquet_compression = 'SNAPPY', 
external_location = 's3://pos-graduacao-lakehouse/curated/faturamento_por_vendedor/' ) 
AS SELECT vendedor, SUM(qtditens * valorunitario) AS faturamento 
FROM trusted GROUP BY vendedor;