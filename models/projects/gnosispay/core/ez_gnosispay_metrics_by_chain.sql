{{
    config(
        materialized="table",
        snowflake_warehouse="GNOSISPAY",
        database="gnosispay",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select
    date::date as date,
    'optimism' as chain,
    sum(transfer_volume) as transfer_volume
from {{ ref('fact_gnosispay_transfers') }}
group by 1