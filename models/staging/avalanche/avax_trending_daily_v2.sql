{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_MD") }}

with
    avax_contracts as (
        select address, name, artemis_application_id as namespace, friendly_name, artemis_category_id as category
        from {{ ref("dim_all_addresses_labeled_gold") }}
        where chain = 'avalanche'
    ),
    prices as ({{ get_coingecko_price_for_trending("avalanche-2") }}),
    last_2_day as (
        select
            t.to_address to_address,
            from_address,
            date_trunc('day', block_timestamp) date,
            tx_fee,
            prices.price,
            avax_contracts.name,
            avax_contracts.namespace,
            avax_contracts.friendly_name,
            case
                when avax_contracts.category is not null
                then avax_contracts.category
                when t.input_data = '0x'
                then 'EOA'
                else null
            end as category
        from avalanche_flipside.core.fact_transactions as t
        left join avax_contracts on lower(t.to_address) = lower(avax_contracts.address)
        left join prices on date_trunc('day', block_timestamp) = prices.date
        where
            t.to_address is not null
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
