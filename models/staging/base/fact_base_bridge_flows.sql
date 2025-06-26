{{
    config(
        materialized="table",
        snowflake_warehouse="BASE",
    )
}}

with
    volume_and_fees_by_chain_and_symbol as (
        select
            date_trunc('day', block_timestamp) as date
            , source_chain
            , destination_chain
            , case when contains(coalesce(lower(t.source_token_symbol), lower(t.destination_token_symbol)), 'usd') then 'Stablecoin' else 'Token' end as category
            , amount_usd
        from {{ ref("fact_base_bridge_transfers") }} t
    )

select
    date,
    'base' as app,
    source_chain,
    destination_chain,
    category,
    coalesce(sum(amount_usd), 0) as amount_usd,
    null as fee_usd
from volume_and_fees_by_chain_and_symbol
group by 1, 2, 3, 4, 5
order by date asc, source_chain asc
