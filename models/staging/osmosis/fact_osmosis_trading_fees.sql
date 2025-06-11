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
            block_date::date as date,
            sum(fees) as gas,
            currency
        from osmosis_flipside.defi.fact_pool_fee_day
        WHERE currency NOT IN (
            'ibc/A23E590BA7E0D808706FB5085A449B3B9D6864AE4DDE7DAF936243CEBB2A3D43'
            , 'ibc/5F5B7DA5ECC80F6C7A8702D525BB0B74279B1F7B8EFAE36E423D68788F7F39FF'
            , 'factory/osmo1z0qrq605sjgcqpylfl4aa6s90x738j7m58wyatt0tdzflg2ha26q67k743/wbtc'
            , 'factory/osmo1q77cw0mmlluxu0wr29fcdd0tdnh78gzhkvhe4n6ulal9qvrtu43qtd0nh8/wiha'
            , 'factory/osmo19hdqma2mj0vnmgcxag6ytswjnr8a3y07q7e70p/wLIBRA'
            , 'factory/osmo1q77cw0mmlluxu0wr29fcdd0tdnh78gzhkvhe4n6ulal9qvrtu43qtd0nh8/turd'
            , 'ibc/DDF1CD4CDC14AE2D6A3060193624605FF12DEE71CF1F8C19EEF35E9447653493'
            , 'factory/osmo10n8rv8npx870l69248hnp6djy6pll2yuzzn9x8/BADKID'
            , 'factory/osmo1s6ht8qrm8x0eg8xag5x3ckx9mse9g4se248yss/BERNESE'
            , 'factory/osmo1pfyxruwvtwk00y8z06dh2lqjdj82ldvy74wzm3/WOSMO'
        )
        {% if is_incremental() %}
            and block_date::date >= (select dateadd('day', -3, max(date)) from {{ this }})
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
            gas_adj * coalesce(t3.price, t2.price, 0) as trading_fees
        from data
        left join prices t2 on data.date = t2.date and lower(data.currency) = lower(t2.currency)
        left join coingecko_price t3 on data.date=t3.date and lower(data.currency) = lower(t3.currency)
        where trading_fees < 10000
    )
select date, 'osmosis' as chain, sum(trading_fees) as trading_fees
from by_token
group by date
order by date desc
