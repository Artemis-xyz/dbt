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
    , 'veax' as artemis_id
    , 'near' as chain

    -- Standardized Metrics    
    
    -- Usage Data
    , veax_tvl.tvl as tvl

from veax_tvl
where veax_tvl.date < to_date(sysdate())