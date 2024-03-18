{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_LG") }}

with
    eth_contracts as (
        select address, name, app as namespace, friendly_name, category
        from {{ ref("dim_contracts_gold") }}
        where chain = 'ethereum'
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    last_2_month as (
        select
            t.to_address to_address,
            from_address,
            date_trunc('day', block_timestamp) date,
            tx_fee,
            price,
            eth_contracts.name,
            eth_contracts.namespace,
            eth_contracts.friendly_name,
            case
                when eth_contracts.category is not null
                then eth_contracts.category
                when t.input_data = '0x'
                then 'EOA'
                else null
            end as category
        from ethereum_flipside.core.fact_transactions as t
        left join eth_contracts on lower(t.to_address) = lower(eth_contracts.address)
        left join prices on date = prices.date
        where
            t.to_address is not null
            and t.block_timestamp >= dateadd(day, -60, current_date)
    ),
    last_week as (
        select
            to_address to_address,
            count(*) as txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(tx_fee * price) as gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_month
        where to_address is not null and date >= dateadd(day, -7, current_date)
        group by to_address
    ),
    two_week as (
        select
            to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(tx_fee * price) as gas_usd
        from last_2_month
        where
            to_address is not null
            and date < dateadd(day, -7, current_date)
            and date >= dateadd(day, -14, current_date)
        group by to_address
    ),
    trending_week as (
        select
            last_week.to_address,
            last_week.txns,
            last_week.gas,
            last_week.gas_usd,
            last_week.dau,
            two_week.txns prev_txns,
            two_week.gas prev_gas,
            two_week.gas_usd prev_gas_usd,
            two_week.dau prev_dau,
            last_week.name,
            last_week.namespace,
            last_week.friendly_name,
            last_week.category,
            'weekly' as granularity
        from last_week
        left join two_week on lower(last_week.to_address) = lower(two_week.to_address)
    ),
    last_month as (
        select
            to_address to_address,
            count(*) as txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(tx_fee * price) as gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_month
        where to_address is not null and date >= dateadd(day, -30, current_date)
        group by to_address
    ),
    two_month as (
        select
            to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(tx_fee * price) as gas_usd
        from last_2_month
        where
            to_address is not null
            and date < dateadd(day, -30, current_date)
            and date >= dateadd(day, -60, current_date)
        group by to_address
    ),
    trending_month as (
        select
            last_month.to_address,
            last_month.txns,
            last_month.gas,
            last_month.gas_usd,
            last_month.dau,
            two_month.txns prev_txns,
            two_month.gas prev_gas,
            two_month.gas_usd prev_gas_usd,
            two_month.dau prev_dau,
            last_month.name,
            last_month.namespace,
            last_month.friendly_name,
            last_month.category,
            'monthly' as granularity
        from last_month
        left join
            two_month on lower(last_month.to_address) = lower(two_month.to_address)
    )
select *
from trending_week
union
select *
from trending_month
