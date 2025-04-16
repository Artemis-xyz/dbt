{{ config(materialized="incremental", unique_key="date") }}

with
    coingecko_eth_price as (
        {{ get_coingecko_price_with_latest('ethereum') }}
    ),
    coingecko_price as (
        select 
            -- This the currency address of allETH that can often times caused problems. Should be priced the same as regular ETH
            -- Part of proposal: https://polkachu.com/gov_proposals/4059
            'factory/osmo1k6c8jln7ejuqwtqmay3yvzrg3kueaczl96pk067ldg8u835w0yhsw27twm/alloyed/allETH' as currency,
            *
        from coingecko_eth_price
    ),
    data as (
        select
            to_date(block_timestamp) as date,
            sum(regexp_substr(fee, '^[0-9]+')::number) as gas,
            substring(fee, length(regexp_substr(fee, '^[0-9]+')) + 1) as currency
        from osmosis_flipside.core.fact_transactions
        {% if is_incremental() %}
            where to_date(block_timestamp) >= (select dateadd('day', -3, max(date)) from {{ this }})
        {% endif %}
        group by date, currency
    ),
    prices as (
        select
            trunc(recorded_hour, 'day') as date, 
            currency, 
            avg(price) as price, 
            case 
                when currency = 'factory/osmo1z0qrq605sjgcqpylfl4aa6s90x738j7m58wyatt0tdzflg2ha26q67k743/wbtc' then 8 --error in underlying pricing table
                when currency = 'factory/osmo1k6c8jln7ejuqwtqmay3yvzrg3kueaczl96pk067ldg8u835w0yhsw27twm/alloyed/allETH' then 18
                else decimal
            end as decimal
        from osmosis_flipside.price.ez_prices
        inner join osmosis_flipside.core.dim_tokens on lower(currency) = lower(address)
        group by currency, date, decimal
    ),
    by_token as (
        select
            data.date,
            data.currency,
            coalesce(gas, 0) / pow(10, t2.decimal) as gas_adj,
            gas_adj * coalesce(t3.price, t2.price, 0) as gas_usd
        from data
        left join prices t2 on data.date = t2.date and data.currency = t2.currency
        left join coingecko_price t3 on data.date=t3.date and data.currency = t3.currency
        where gas_usd < 10000
    )
select date, 'osmosis' as chain, sum(gas_usd) as gas_usd
from by_token
group by date
order by date desc
