{{
    config(
        materialized='table',
        snowflake_warehouse='PHARAOH',
        database='PHARAOH',
        schema='core',
        alias='ez_metrics'
    )
}}

with pharaoh_tvl as (
    {{ get_defillama_protocol_tvl('pharaoh') }}
)
, pharaoh_market_data as (
    {{ get_coingecko_metrics('pharaoh') }}
)

select
    pharaoh_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , pharaoh_tvl.tvl

    -- Market Metrics
    , pmd.price
    , pmd.market_cap
    , pmd.fdmc
    , pmd.token_turnover_circulating
    , pmd.token_turnover_fdv
    , pmd.token_volume
from pharaoh_tvl
left join pharaoh_market_data pmd using (date)
where pharaoh_tvl.date < to_date(sysdate())