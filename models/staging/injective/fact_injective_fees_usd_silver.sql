{{config(snowflake_warehouse="INJECTIVE")}}

with
    unique_tokens as (
        select distinct (coingecko_id)
        from {{ source("PROD", "fact_injective_fees_native_all_silver") }}
    ),
    prices as (
        select date as price_date, shifted_token_price_usd as price, coingecko_id
        from {{ source("PROD", "fact_coingecko_token_date_adjusted_gold") }}
        where coingecko_id in (select * from unique_tokens)
        union
        select
            dateadd('day', -1, date) as price_date,
            token_current_price as price,
            token_id as coingecko_id
        from {{ source("PROD", "fact_coingecko_token_realtime_data") }}
        where token_id in (select * from unique_tokens)
    ),
    collapsed_prices as (
        select price_date as date, max(price) as price, coingecko_id
        from prices
        group by coingecko_id, date
    )
select f.date as date, sum(p.price * f.fees_native_all) as fees, 'injective' as chain
from {{ source("PROD", "fact_injective_fees_native_all_silver") }} f
left join collapsed_prices p on f.date = p.date and f.coingecko_id = p.coingecko_id
group by f.date
order by f.date desc
