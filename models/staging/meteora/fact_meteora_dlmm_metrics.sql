{{ config(materialized="table") }}

with artemis_tvl as (
  select
    date,
    sum(balance) as tvl
  from
    {{ref('fact_meteora_dlmm_tvl')}}
  group by 1
)
, artemis_fees as (
  select
    date,
    sum(swap_fee_amount_usd) as dlmm_spot_fees
  from
    {{ref('fact_meteora_decoded_swaps_extract')}}
  group by 1
)
, swap_metrics as (
  select
    date,
    unique_traders,
    number_of_swaps,
    trading_volume
  from {{ref('fact_meteora_dlmm_swap_metrics')}}
)
  
select
  date,
  artemis_tvl.tvl as tvl,
  artemis_fees.dlmm_spot_fees as dlmm_spot_fees,
  swap_metrics.unique_traders as unique_traders,
  swap_metrics.number_of_swaps as number_of_swaps,
  swap_metrics.trading_volume as trading_volume
from artemis_tvl
left join artemis_fees using(date)
left join swap_metrics using(date)
