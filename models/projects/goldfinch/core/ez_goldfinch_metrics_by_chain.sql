{{
    config(
        materialized="view"
        , snowflake_warehouse="GOLDFINCH"
        , database="goldfinch"
        , schema="core"
        , alias="ez_metrics_by_chain"
    )
}}

select
    'ethereum' as chain
    , *
from {{ ref('ez_goldfinch_metrics') }}
