{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}

with
    near_contracts as (
        select address, name, artemis_application_id as namespace, friendly_name, artemis_category_id as category
        from {{ ref("dim_all_addresses_labeled_gold") }}
        where chain = 'near'
    ),
    prices as ({{ get_coingecko_price_for_trending("near") }}),
    last_2_month as (
        select
            t.tx_receiver to_address,
            tx_signer from_address,
            date_trunc('day', block_timestamp) date,
            transaction_fee / pow(10, 24) tx_fee,
            prices.price,
            near_contracts.name,
            near_contracts.namespace,
            near_contracts.friendly_name,
            case
                when near_contracts.category is not null
                then near_contracts.category
                when (tx:"actions"[0]:"Transfer":"deposit" / pow(10, 24) > .02)
                then 'EOA'
                else null
            end as category
        from near_flipside.core.fact_transactions as t
        left join near_contracts on lower(t.tx_receiver) = lower(near_contracts.address)
        left join prices on date_trunc('day', block_timestamp) = prices.date
        where
            t.tx_receiver is not null
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
