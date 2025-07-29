{{
    config(
        materialized='table',
        snowflake_warehouse='STELLASWAP',
        database='STELLASWAP',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with stellaswap_tvl as (
    {{ get_defillama_protocol_tvl('stellaswap') }}
)

select
    stellaswap_tvl.date
    , 'stellaswap' as artemis_id
    , 'Defillama' as source
    , 'moonbeam' as chain

    -- Standardized Metrics
    -- Usage Metrics
    , stellaswap_tvl.tvl
from stellaswap_tvl
where stellaswap_tvl.date < to_date(sysdate())  