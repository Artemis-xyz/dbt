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
    tt.date
    , 'Defillama' as source
    , 'thorchain' as chain

    -- Standardized Metrics
    , tt.tvl

from thorchain_tvl tt
where tt.date < to_date(sysdate())
and tt.name = 'thorchain' -- macro above returns data for 'Thorchain Lending' too, so we filter by name