{{
    config(
        materialized='table',
        snowflake_warehouse='BITFLOW',
        database='BITFLOW',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with bitflow_tvl as (
    {{ get_defillama_protocol_tvl('bitflow') }}
)
select
    bitflow_tvl.date,
    'Defillama' as source,
    'stacks' as chain,
    bitflow_tvl.tvl
from bitflow_tvl
where bitflow_tvl.date < to_date(sysdate())