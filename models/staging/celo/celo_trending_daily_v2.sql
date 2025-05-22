{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}

with
    last_2_day as (
        select
            t.contract_address to_address,
            from_address,
            date_trunc('day', block_timestamp) date,
            tx_fee,
            gas_usd, 
            name,
            app as namespace,
            friendly_name,
            category
        from {{ref('fact_celo_transactions_v2')}} as t
        where
            t.contract_address is not null
            and t.block_timestamp >= dateadd(day, -2, current_date)
    ),
    last_day as (
        select
            t.to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_day as t
        where t.to_address is not null and t.date >= dateadd(day, -1, current_date)
        group by to_address
    ),
    two_days as (
        select
            t.to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd
        from last_2_day as t
        where
            t.to_address is not null
            and t.date < dateadd(day, -1, current_date)
            and t.date >= dateadd(day, -2, current_date)
        group by to_address
    )
select
    last_day.to_address,
    last_day.txns,
    last_day.gas,
    last_day.gas_usd,
    last_day.dau,
    two_days.txns prev_txns,
    two_days.gas prev_gas,
    two_days.gas_usd prev_gas_usd,
    two_days.dau prev_dau,
    last_day.name,
    last_day.namespace,
    last_day.friendly_name,
    last_day.category,
    'daily' as granularity
from last_day
left join two_days on lower(last_day.to_address) = lower(two_days.to_address)
