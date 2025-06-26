{{ config(
    materialized="table",
    warehouse="USDT0",
    database="USDT0",
    schema="core",
    alias="ez_metrics"
) }}

with raw_data as (
    select
        src_block_timestamp::date as date,
        count(distinct src_address) as bridge_dau,
        count(*) as bridge_txns,
        sum(amount_sent) as bridge_volume,
    from {{ ref("fact_usdt0_transfers") }}
    group by date
)
select
    date,
    'usdt0' as app,
    'Bridge' as category,
    bridge_dau,
    bridge_txns,
    bridge_volume
from raw_data
where date < to_date(sysdate())
