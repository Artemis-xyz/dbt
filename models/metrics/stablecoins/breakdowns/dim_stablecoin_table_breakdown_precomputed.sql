{{ config(materialized='table', snowflake_warehouse='STABLECOIN_V2_LG') }}

WITH chain_ranking AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY chain ORDER BY stablecoin_supply DESC) as rank
  FROM 
    dim_stablecoin_table_breakdown
), symbol_ranking AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY stablecoin_supply DESC) as rank
  FROM 
    dim_stablecoin_table_breakdown
)
SELECT 
    * exclude(rank)
FROM 
    chain_ranking
WHERE 
    rank <= 500

UNION

SELECT 
    * exclude(rank)
FROM 
    symbol_ranking
WHERE 
    rank <= 500