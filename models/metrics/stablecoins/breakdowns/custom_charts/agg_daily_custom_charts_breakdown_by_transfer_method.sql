{{ config(materialized='table') }}

with 
raw_stablecoin_data as (
    select 
        date_granularity as date,
        sum(artemis_stablecoin_transfer_volume) as transfer_volume,
    from {{ ref("agg_daily_stablecoin_breakdown_symbol") }}
    group by 1
)
, stablecoins as (
    select
        date,
        avg(transfer_volume) over (
            order by date 
            rows between 29 preceding and current row
        ) as transfer_volume
    from raw_stablecoin_data
)
, data as (
select date, transfer_volume, 'visa' as transfer_method from {{ref('ez_visa_metrics')}}
union all
select date, transfer_volume, 'paypal' as transfer_method from {{ref('ez_paypal_metrics')}}
union all
select date, transfer_volume, 'remittance' as transfer_method from {{ref('ez_remittance_metrics')}}
union all
select date, transfer_volume, 'ach' as transfer_method from {{ref('ez_ach_metrics')}}
union all
select date, transfer_volume, 'stablecoins' as transfer_method from stablecoins
)
select 
    date as date_granularity,
    transfer_method,
    transfer_volume as stablecoin_transfer_volume
from data
where date >= '2020-01-01'
order by date desc