{{
    config(
        materialized="table",
        database = 'SAFE',
        schema = 'core',
        snowflake_warehouse = 'SAFE',
        alias = 'ez_metrics_by_chain'
    )
}}

with tvl as (
    select
        date
        , chain
        , tvl
    from {{ ref("fact_safe_tvl_by_chain") }}
)
, safes_created as (
    select
        date
        , chain
        , safes_created
    from {{ ref("fact_safe_daily_safes_created") }}
)

select
    tvl.date
    , tvl.chain
    , safes_created as multisigs_created
    , tvl as value_in_multisigs
from tvl
left join safes_created using (date, chain)
