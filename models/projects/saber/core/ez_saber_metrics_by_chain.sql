{{
    config(
        materialized='table',
        snowflake_warehouse='SABER',
        database='SABER',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with saber_tvl as (
    {{ get_defillama_protocol_tvl('saber') }}
)
select
    saber_tvl.date
    , 'saber' as artemis_id
    , 'Defillama' as source
    , 'solana' as chain

    --Usage Data
    , saber_tvl.tvl

from saber_tvl
where saber_tvl.date < to_date(sysdate())