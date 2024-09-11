
with prices as (
    select
        date(hour) as date,
        avg(price) as price
    from ethereum_flipside.price.ez_prices_hourly
    where lower(token_address) = lower('0x9e32b13ce7f2e80a01932b42553652e053d6ed8e')
    group by 1
)
select
    f.date,
    f.fees_native * p.price as fees_usd
from {{ref("fact_metis_fees_native")}} f
left join prices p on p.date = f.date
where date(f.date) < to_date(sysdate())