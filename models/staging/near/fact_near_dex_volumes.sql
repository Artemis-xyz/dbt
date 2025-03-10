{{ config(materialized="table", snowflake_warehouse="NEAR") }}

select 
    date_trunc('day', block_timestamp) as date,
    sum(
        case
            when amount_out_usd is not null and amount_in_usd is not null then greatest(amount_out_usd, amount_in_usd)
            when amount_out_usd is not null then amount_out_usd
            when amount_in_usd is not null then amount_in_usd
            else 0
        end
    ) as daily_volume_usd
from near_flipside.defi.ez_dex_swaps
where date_trunc('day', block_timestamp) > '2025-01-01'
group by date
order by date asc 