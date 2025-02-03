{{
    config(
        materialized='view',
        snowflake_warehouse="IOTEX",
        database="iotex",
        schema="core",
        alias="ez_metrics_by_chain"
    )
}}

select
    *
from {{ ref('ez_iotex_metrics') }}