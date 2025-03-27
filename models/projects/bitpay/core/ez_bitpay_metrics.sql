{{
    config(
        materialized="table",
        snowflake_warehouse="BITPAY",
        database="bitpay",
        schema="core",
        alias="ez_metrics",
    )
}}

select
    date::date as date,
    sum(transfer_volume) as transfer_volume
from {{ ref("fact_bitpay_transfers") }}
group by 1