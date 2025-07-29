{{
    config(
        materialized="incremental",
        unique_key=["date", "source_chain", "destination_chain", "category"],
        snowflake_warehouse="SONIC",
    )
}}

select
    date,
    'sonic' as app,
    source_chain,
    destination_chain,
    category,
    symbol,
    sum(amount) as amount_usd,
    null as fee_usd
from {{ ref("fact_sonic_bridge_transfers") }} t
{% if is_incremental() %}
where date >= (
    select dateadd('day', -3, max(date))
    from {{ this }}
)
{% endif %}
group by 1, 2, 3, 4, 5, 6
order by date asc, source_chain asc
