{{
    config(
        materialized='table',
        snowflake_warehouse='CELLANA',
        database='CELLANA',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with cellana_tvl as (
    {{ get_defillama_protocol_tvl('cellana') }}
)

select
    cellana_tvl.date
    , 'Defillama' as source
    , 'aptos' as chain

    -- Standardized Metrics
    , cellana_tvl.tvl
from cellana_tvl
where cellana_tvl.date < to_date(sysdate())