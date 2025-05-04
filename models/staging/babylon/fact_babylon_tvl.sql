{{
    config(
        materialized="table",
        alias="fact_babylon_tvl",
    )
}}

WITH dfillama_tvl AS (
  select
    *
  FROM
    {{ref('fact_babylon_tvl_defillama')}}
)
, api_tvl as (
  SELECT
    date,
    total_active_tvl_btc,
    total_active_tvl_usd
  FROM
    {{ref('fact_babylon_api_metrics')}}
)
select
  date,
  dfillama_tvl.tvl as tvl
from dfillama_tvl
where date < '2025-05-01'
union all 
select
  api_tvl.date,
  api_tvl.total_active_tvl_usd as tvl
from api_tvl
where api_tvl.date >= '2025-05-01'