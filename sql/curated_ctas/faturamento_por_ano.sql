CREATE TABLE lakehouse_curated.faturamento_por_ano 
WITH ( format = 'PARQUET', parquet_compression = 'SNAPPY', 
external_location = 's3://pos-graduacao-lakehouse/curated/faturamento_por_ano/' ) 
AS SELECT ano, SUM(qtditens * valorunitario) AS faturamento 
FROM trusted GROUP BY ano;