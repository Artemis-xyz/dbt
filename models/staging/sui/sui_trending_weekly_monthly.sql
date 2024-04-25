{{ config(materialized="table", snowflake_warehouse="BAM_TRENDING_WAREHOUSE_LG") }}

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
        where block_time >= dateadd(day, -60, current_date)
        group by transaction_block_digest
    ),
    last_2_month as (
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
        where tb.block_time >= dateadd(day, -60, current_date)
    ),
    last_week as (
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
        from last_2_month as t
        where package is not null and t.date >= dateadd(day, -7, current_date)
        group by package
    ),
    two_week as (
        select
            t.package package,
            count(*) txns,
            count(distinct(sender)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd
        from last_2_month as t
        where
            package is not null
            and t.date < dateadd(day, -7, current_date)
            and t.date >= dateadd(day, -14, current_date)
        group by t.package
    ),
    trending_week as (
        select
            last_week.package as to_address,
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
        left join two_week on lower(last_week.package) = lower(two_week.package)
    ),
    last_month as (
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
        from last_2_month as t
        where package is not null and t.date >= dateadd(day, -30, current_date)
        group by package
    ),
    two_month as (
        select
            t.package package,
            count(*) txns,
            count(distinct(sender)) dau,
            sum(tx_fee) as gas,
            sum(gas_usd) as gas_usd
        from last_2_month as t
        where
            package is not null
            and t.date < dateadd(day, -30, current_date)
            and t.date >= dateadd(day, -60, current_date)
        group by t.package
    ),
    trending_month as (
        select
            last_month.package as to_address,
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
            two_month on lower(last_month.package) = lower(two_month.package)
    )
select *
from trending_week
union
select *
from trending_month