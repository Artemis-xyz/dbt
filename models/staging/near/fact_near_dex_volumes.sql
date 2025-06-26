{{ config(materialized="table", snowflake_warehouse="NEAR") }}

with flipside_near_dex_volumes as (
    select 
        date_trunc('day', block_timestamp) as date,
        sum(
            case
                when amount_out_usd is not null and amount_in_usd is not null then greatest(amount_out_usd, amount_in_usd)
                when amount_out_usd is not null then amount_out_usd
                when amount_in_usd is not null then amount_in_usd
                else 0
            end
        ) as volume_usd
    from {{ source("NEAR_FLIPSIDE", "ez_dex_swaps") }}
    where platform <> 'ref-finance.near' and platform <> 'v2.ref-finance.near'
    group by date
    order by date asc
), 

rhea_near_dex_volumes as (
    select 
        date as date,
        volume as volume_usd
    from {{ ref('fact_near_rhea_finance_volumes') }}
)

select 
    coalesce(flipside.date, rhea.date) as date,
    sum(coalesce(flipside.volume_usd,0) + coalesce(rhea.volume_usd,0)) as volume_usd
from flipside_near_dex_volumes as flipside
full outer join rhea_near_dex_volumes as rhea
    on flipside.date = rhea.date
group by 1
