{{ config(materialized="table") }}
with
    dau_txns as (
        select
            block_timestamp::date as date,
            count(distinct from_address) as dau,
            count(distinct transaction_hash) as txns
        from {{ ref("fact_celo_transactions") }}
        where receipt_status = 1
        group by 1
    ),
    prices as (
        select date, 'celo' as fee_currency, price as price
        from ({{ get_coingecko_price_with_latest("celo") }})
        union all
        select
            date,
            '0x765de816845861e75a25fca122bb6898b8b1282a' as fee_currency,
            price as price
        from ({{ get_coingecko_price_with_latest("celo-dollar") }})
        union all
        select
            date,
            '0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73' as fee_currency,
            price as price
        from ({{ get_coingecko_price_with_latest("celo-euro") }})
    ),
    native_gas as (
        select
            block_timestamp::date as date,
            case
                when fee_currency is null then 'celo' else fee_currency
            end as fee_currency,
            sum((receipt_gas_used * gas_price) / 1E18) as gas
        from {{ ref("fact_celo_transactions") }}
        group by 1, 2
    ),
    gas_usd as (
        select native_gas.date, sum(native_gas.gas * prices.price) as gas_usd
        from native_gas
        join
            prices
            on native_gas.date = prices.date
            and native_gas.fee_currency = prices.fee_currency
        group by 1
    ),
    burned as (
        select
            block_timestamp::date as date,
            sum(coalesce((gas_used * base_fee_per_gas) / 1E18, 0)) as revenue_native,
        from {{ ref("fact_celo_blocks") }}
        where block_timestamp::date > '2022-03-08'
        group by date
    )
select
    dau_txns.date,
    dau_txns.dau,
    dau_txns.txns,
    gas_usd.gas_usd,
    coalesce(burned.revenue_native, 0) as revenue_native,
    coalesce(revenue_native * price, 0) as revenue,
    coalesce(gas_usd.gas_usd / txns, 0) as avg_txn_fee,
    'celo' as chain
from dau_txns
join gas_usd on dau_txns.date = gas_usd.date
left join burned on dau_txns.date = burned.date
left join
    (select date, price from prices where fee_currency = 'celo') as prices
    on dau_txns.date = prices.date