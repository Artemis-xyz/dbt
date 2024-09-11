with prices as (
    select 
        date,
        shifted_token_price_usd as price 
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where coingecko_id = 'flare-networks'
)
select
    f.date,
    f.fees_native * p.price as fees_usd
from {{ref("fact_flare_fees_native")}} f
left join prices p on p.date = f.date
where f.date < to_date(sysdate())