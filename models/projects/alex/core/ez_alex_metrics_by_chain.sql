{{
    config(
        materialized='table',
        snowflake_warehouse='ALEX',
        database='ALEX',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with alex_tvl as (
    {{ get_defillama_protocol_tvl('alex') }}
)


select
    alex_tvl.date
    , 'alex' as artemis_id
    , 'Defillama' as source
    , 'stacks' as chain

    -- Standardized Metrics
    , alex_tvl.tvl as spot_tvl
    , alex_tvl.tvl as tvl

from alex_tvl
where alex_tvl.date < to_date(sysdate())