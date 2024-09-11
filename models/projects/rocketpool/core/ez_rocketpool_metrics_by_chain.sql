{{
    config(
        materialized="view",
        snowflake_warehouse="ROCKETPOOL",
        database="rocketpool",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select 
    'ethereum' as chain,
    *
from {{ ref('ez_rocketpool_metrics') }}