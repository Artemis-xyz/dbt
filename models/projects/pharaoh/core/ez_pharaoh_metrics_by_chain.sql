{{
    config(
        materialized='table',
        snowflake_warehouse='PHARAOH',
        database='PHARAOH',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with pharaoh_tvl as (
    {{ get_defillama_protocol_tvl('pharaoh') }}
)

select
    pharaoh_tvl.date
    , 'Defillama' as source
    , 'avalanche' as chain

    -- Standardized Metrics
    , pharaoh_tvl.tvl
from pharaoh_tvl
where pharaoh_tvl.date < to_date(sysdate())