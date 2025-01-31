{{
    config(
        materialized='table',
        snowflake_warehouse='CETUS',
        database='CETUS',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with cetus_tvl as (
    {{ get_defillama_protocol_tvl('cetus') }}
)

select
    cetus_tvl.date,
    'Defillama' as source,
    'sui' as chain,
    cetus_tvl.tvl
from cetus_tvl
where cetus_tvl.date < to_date(sysdate())