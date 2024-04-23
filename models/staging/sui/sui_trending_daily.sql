{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_MD") }}

with
    sui_contracts as (

        select address, name, app as namespace, friendly_name, category
        from {{ ref("dim_contracts_gold") }}
        where chain = 'sui'
    ),
    prices as ({{ get_coingecko_price_for_trending("sui") }}),
    sui_transactions as (
        select 
            transaction_block_digest as transaction_block_digest,
            min_by(package, index) as package,
            array_agg(type) as type_array
        from {{ source('ZETTABLOCKS_SUI', 'transactions') }}
        where block_time >= dateadd(day, -2, current_date)
        group by transaction_block_digest
    ),
    last_2_day as (
        select 
            digest as tx_hash,
            date_trunc('day', tb.block_time) as date, 
            sender,
            (total_gas_cost + storage_rebate - storage_cost)/10e8 as tx_fee,
            (total_gas_cost + storage_rebate - storage_cost)/10e8 * price as gas_usd,
            package,
            sui_contracts.name,
            sui_contracts.namespace,
            sui_contracts.friendly_name,
            case 
                when package is null and array_size(type_array) = 2 and ARRAY_CONTAINS('TransferObjects'::variant, type_array)
                then 'EOA'
                else sui_contracts.category 
            end as category,
            'sui' as chain,
            status
        from {{ source('ZETTABLOCKS_SUI', 'transaction_blocks') }} as tb 
        left join sui_transactions as t on lower(digest) = lower(transaction_block_digest)
        left join sui_contracts on lower(package) = lower(address)
        left join prices on date = prices.date
        where tb.block_time >= dateadd(day, -2, current_date)
    ),
    last_day as (
        select
            package,
            count(*) txns,
            count(distinct(sender)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd,
            max(name) name,
            max(namespace) namespace,
            max(friendly_name) friendly_name,
            max(category) category
        from last_2_day as t
        where package is not null and t.date >= dateadd(day, -1, current_date)
        group by package
    ),
    two_days as (
        select
            t.package package,
            count(*) txns,
            count(distinct(sender)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd
        from last_2_day as t
        where
            package is not null
            and t.date < dateadd(day, -1, current_date)
            and t.date >= dateadd(day, -2, current_date)
        group by t.package
    )
select
    last_day.package as to_address,
    last_day.txns,
    last_day.gas,
    last_day.gas_usd gas_usd,
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
left join two_days on lower(last_day.package) = lower(two_days.package)
