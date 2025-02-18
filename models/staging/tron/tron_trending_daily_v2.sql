{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE") }}

with
    tron_contracts as (
        select address, name, artemis_application_id as namespace, friendly_name, artemis_category_id as category
        from {{ ref("dim_all_addresses_labeled_gold") }}
        where chain = 'tron'
    ),
    tron_fees as (
        select
            concat('0x', transaction_hash) as tx_hash,
            max(amount) as gas,
            max(usd_amount) as gas_usd,
            max(usd_exchange_rate) as price
        from tron_allium.assets.trx_token_transfers
        where
            transfer_type = 'fees' and block_timestamp >= dateadd(day, -2, current_date)
        group by transaction_hash
    ),
    last_2_day as (
        select
            hash as tx_hash,
            to_address,
            t.from_address,
            date_trunc('day', block_timestamp) date,
            fees.gas::double as tx_fee,
            fees.gas_usd::double as gas_usd,
            'tron' as chain,
            tron_contracts.name,
            tron_contracts.namespace,
            tron_contracts.friendly_name,
            case
                when tron_contracts.category in ('CeFi', 'Stablecoin', 'DeFi')
                then tron_contracts.category
                when t.input = '0x' and t.value::double > 0
                then 'EOA'
                when tron_contracts.category is not null
                then tron_contracts.category
                else null
            end as category
        from tron_allium.raw.transactions as t
        left join tron_fees as fees on t.hash = fees.tx_hash
        left join tron_contracts on lower(t.to_address) = lower(tron_contracts.address)
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
            sum(gas_usd) as gas_usd,
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
            sum(gas_usd) as gas_usd
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
