CREATE TABLE lakehouse_curated.qtd_vendas_por_vendedor
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://pos-graduacao-lakehouse/curated/qtd_vendas_por_vendedor/'
) AS
SELECT
  vendedor,
  COUNT(DISTINCT nfe) AS qtd_vendas
FROM trusted
GROUP BY vendedor;