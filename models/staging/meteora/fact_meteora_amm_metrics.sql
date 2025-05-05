{{
    config(
        materialized="table",
        alias="fact_meteora_amm_metrics",
    )
}}

WITH defillama_fees AS (
  select
    *
  FROM
    {{ref('fact_meteora_amm_defillama_fees')}}
)
, api_fees as (
  SELECT
    date,
    amm_daily_fee
  FROM
    {{ref('fact_meteora_amm_api_metrics')}}
)
select
  date,
  defillama_fees.fees as fees
from defillama_fees
where date < '2025-05-01'
union all 
select
  api_fees.date,
  api_fees.amm_daily_fee as fees
from api_fees
where api_fees.date >= '2025-05-01'