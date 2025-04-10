{{
    config(
        materialized="incremental",
        snowflake_warehouse= "STABLECOIN_DAILY",
    )
}}

select
    b.date,
    b.symbol,
    sum(b.stablecoin_supply) as stablecoin_supply
from {{ ref("agg_daily_stablecoin_breakdown_silver") }} b
{% if is_incremental() %}
    where b.date >= (select dateadd('day', -7, max(date)) from {{ this }})
{% endif %}
group by 1,2
