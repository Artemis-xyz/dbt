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
, swap_metrics as (
    select
        date,
        unique_traders,
        number_of_swaps,
        trading_volume
    from {{ ref('fact_meteora_amm_swap_metrics') }}
)
select
  date,
  defillama_fees.fees as fees,
  swap_metrics.unique_traders as unique_traders,
  swap_metrics.number_of_swaps as number_of_swaps,
  swap_metrics.trading_volume as trading_volume,
from defillama_fees
left join swap_metrics using(date)
where date < '2025-05-01'
union all 
select
  api_fees.date,
  api_fees.amm_daily_fee as fees,
  swap_metrics.unique_traders as unique_traders,
  swap_metrics.number_of_swaps as number_of_swaps,
  swap_metrics.trading_volume as trading_volume,
from api_fees
left join swap_metrics using(date)
where api_fees.date >= '2025-05-01'