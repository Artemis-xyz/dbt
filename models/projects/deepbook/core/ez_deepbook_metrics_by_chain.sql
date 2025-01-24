{{
    config(
        materialized='table',
        snowflake_warehouse='DEEPBOOK',
        database='DEEPBOOK',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with deepbook_tvl as (
    {{ get_defillama_protocol_tvl('deepbook') }}
)

select
    deepbook_tvl.date,
    'Defillama' as source,
    'sui' as chain,
    deepbook_tvl.tvl
from deepbook_tvl
where deepbook_tvl.date < to_date(sysdate())