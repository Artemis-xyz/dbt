with
    data as (
        select
            to_date(block_timestamp) as date,
            sum(regexp_substr(fee, '^[0-9]+')::number) as gas,
            substring(fee, length(regexp_substr(fee, '^[0-9]+')) + 1) as currency
        from osmosis_flipside.core.fact_transactions
        group by date, currency
    ),
      prices as (
        select
            trunc(recorded_hour, 'day') as date, 
            currency, 
            avg(price) as price, 
            case 
                when currency = 'factory/osmo1z0qrq605sjgcqpylfl4aa6s90x738j7m58wyatt0tdzflg2ha26q67k743/wbtc' then 1E8 --error in underlying pricing table
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
            gas_adj * coalesce(price, 0) as gas_usd
        from data
        inner join prices t2 on data.date = t2.date and data.currency = t2.currency
    )
select date, 'osmosis' as chain, sum(gas_usd) as gas_usd
from by_token
group by date
order by date desc