{{
    config(
        materialized="table",
        snowflake_warehouse="BRIDGE_MD",
    )
}}

select
    date_trunc('day', hour) as date,
    'across' as app,
    source_chain,
    destination_chain,
    coalesce(destination_category, 'Not Categorized') as category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from
    {{ ref("fact_across_transfers_with_price") }}
WHERE amount_usd < 100000000
group by date_trunc('day', hour), source_chain, destination_chain, category
