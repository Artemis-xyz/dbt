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
    pharaoh_tvl.date,
    'Defillama' as source,
    pharaoh_tvl.tvl,
    pharaoh_market_data.price,
    pharaoh_market_data.market_cap,
    pharaoh_market_data.fdmc,
    pharaoh_market_data.token_turnover_circulating,
    pharaoh_market_data.token_turnover_fdv,
    pharaoh_market_data.token_volume
from pharaoh_tvl
left join pharaoh_market_data using (date)
where pharaoh_tvl.date < to_date(sysdate())