{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUIDSWAP',
        database='LIQUIDSWAP',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with liquidswap_tvl as (
    {{ get_defillama_protocol_tvl('liquidswap') }}
)

select
    liquidswap_tvl.date
    , 'Defillama' as source
    , 'aptos' as chain

    -- Standardized Metrics
    , liquidswap_tvl.tvl
    
from liquidswap_tvl
where liquidswap_tvl.date < to_date(sysdate())