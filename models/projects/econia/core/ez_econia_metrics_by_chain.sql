{{
    config(
        materialized='table',
        snowflake_warehouse='ECONIA',
        database='ECONIA',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with econia_tvl as (
    {{ get_defillama_protocol_tvl('econia') }}
)

select
    econia_tvl.date
    , 'Defillama' as source
    , 'aptos' as chain

    -- Standardized Metrics
    , econia_tvl.tvl
from econia_tvl
where econia_tvl.date < to_date(sysdate())