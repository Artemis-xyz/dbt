{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}

with
    near_contracts as (

        select address, name, artemis_application_id as namespace, friendly_name, artemis_category_id AS category
        from {{ ref("dim_all_addresses_labeled_gold") }}
        where chain = 'near'
    ),
    prices as ({{ get_coingecko_price_for_trending("near") }}),
    last_2_day as (
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
            and t.block_timestamp >= dateadd(day, -2, current_date)
    ),
    last_day as (
        select
            t.to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(tx_fee * price) as gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_day as t
        where t.to_address is not null and t.date >= dateadd(day, -1, current_date)
        group by t.to_address
    ),
    two_days as (
        select
            t.to_address to_address,
            count(*) txns,
            count(distinct(from_address)) dau,
            sum(tx_fee) as gas,
            sum(tx_fee * price) as gas_usd
        from last_2_day as t
        where
            t.to_address is not null
            and t.date < dateadd(day, -1, current_date)
            and t.date >= dateadd(day, -2, current_date)
        group by t.to_address
    )
select
    last_day.to_address,
    last_day.txns txns,
    last_day.gas gas,
    last_day.gas_usd gas_usd,
    last_day.dau dau,
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
