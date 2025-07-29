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
    , 'cellana' as artemis_id
    , 'Defillama' as source
    , 'aptos' as chain

    -- Standardized Metrics

    -- Usage Data
    , cellana_tvl.tvl as tvl

from cellana_tvl
where cellana_tvl.date < to_date(sysdate())