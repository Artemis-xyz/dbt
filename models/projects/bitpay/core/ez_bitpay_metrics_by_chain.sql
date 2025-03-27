{{
    config(
        materialized="table",
        snowflake_warehouse="BITPAY",
        database="bitpay",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

select
    date::date as date,
    'ethereum' as chain,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_bitpay_transfers") }}
group by 1