{{
    config(
        materialized="table",
        snowflake_warehouse="EXACTLY",
        database="exactly",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

with
    exa_accounts as (
        select
        distinct '0x' || lower(substr(topic_1, 27, 40)) as address
        from optimism_flipside.core.ez_decoded_event_logs l
        where l.block_timestamp >= try_cast('2024-08-29' as timestamp)
        and lower(topic_0) = lower('0x0b6a8f0ea14435788bae11ec53c2c0f6964bd797ab9a7f1c89773b87127131ba')
    ),
    op as (
        select
            symbol,
            date_trunc('day', date) as day_start,
            sum(transfer_volume) as vol
        from {{ ref("fact_optimism_stablecoin_transfers") }}
        where
            lower(contract_address) = lower('0x0b2c639c533813f4aa9d7837caf62653d097ff85')
        and
            lower(to_address) in (select lower(address) from exa_accounts)
        group by 1, 2
    )
select
    day_start::date as date,
    symbol as token,
    sum(vol) as transfer_volume
from op
group by date, token
order by date desc