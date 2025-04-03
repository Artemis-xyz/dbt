{{
    config(
        materialized='table',
        snowflake_warehouse='VEAX',
        database='VEAX',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with veax_tvl as (
    {{ get_defillama_protocol_tvl('veax') }}
)

select
    veax_tvl.date
    , 'Defillama' as source
    , 'near' as chain

    -- Standardized Metrics
    , veax_tvl.tvl
from veax_tvl
where veax_tvl.date < to_date(sysdate())