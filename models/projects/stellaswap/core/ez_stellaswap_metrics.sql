{{
    config(
        materialized='table',
        snowflake_warehouse='STELLASWAP',
        database='STELLASWAP',
        schema='core',
        alias='ez_metrics'
    )
}}

with stellaswap_tvl as (
    {{ get_defillama_protocol_tvl('stellaswap') }}
)

select
    stellaswap_tvl.date,
    'Defillama' as source,
    stellaswap_tvl.tvl
from stellaswap_tvl
where stellaswap_tvl.date < to_date(sysdate())  