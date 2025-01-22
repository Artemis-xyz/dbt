{{
    config(
        materialized='table',
        snowflake_warehouse='VEAX',
        database='VEAX',
        schema='core',
        alias='ez_metrics'
    )
}}

with veax_tvl as (
    {{ get_defillama_protocol_tvl('veax') }}
)

select
    veax_tvl.date,
    'Defillama' as source,
    veax_tvl.tvl
from veax_tvl