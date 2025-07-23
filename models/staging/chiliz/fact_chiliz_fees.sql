
with prices as (
    select
        date(hour) as date,
        avg(price) as price
    from ethereum_flipside.price.ez_prices_hourly
    where lower(token_address) = lower('0x3506424f91fd33084466f402d5d97f05f8e3b4af')
    group by 1
)
select
    f.date,
    f.fees_native,
    f.fees_native * p.price as fees
from {{ref("fact_chiliz_fees_native")}} f
left join prices p on p.date = f.date
where f.date < to_date(sysdate())