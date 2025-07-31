{{
    config(
        materialized='table',
        snowflake_warehouse='THORCHAIN',
        database='THORCHAIN',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with thorchain_tvl as (
    {{ get_defillama_protocol_tvl('thorchain') }}
)

select
    thorchain_tvl.date
    , 'thorchain' as artemis_id
    , 'thorchain' as chain

    -- Standardized Metrics

    -- Usage Data
    , thorchain_tvl.tvl

from thorchain_tvl
where thorchain_tvl.date < to_date(sysdate())
and thorchain_tvl.name = 'thorchain' -- macro above returns data for 'Thorchain Lending' too, so we filter by name