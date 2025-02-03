{{
    config(
        materialized='table',
        snowflake_warehouse='SABER',
        database='SABER',
        schema='core',
        alias='ez_metrics'
    )
}}

with saber_tvl as (
    {{ get_defillama_protocol_tvl('saber') }}
)
select
    saber_tvl.date,
    'Defillama' as source,
    saber_tvl.tvl
from saber_tvl
where saber_tvl.date < to_date(sysdate())