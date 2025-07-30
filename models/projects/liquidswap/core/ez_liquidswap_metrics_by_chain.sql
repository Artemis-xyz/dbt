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
    , 'liquidswap' as artemis_id
    , 'aptos' as chain

    -- Standardized Metrics
    
    -- Usage Data
    , liquidswap_tvl.tvl
    
from liquidswap_tvl
where liquidswap_tvl.date < to_date(sysdate())