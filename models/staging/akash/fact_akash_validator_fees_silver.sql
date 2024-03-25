with
    fees as (select * from pc_dbt_db.prod.fact_akash_validator_fees_native_silver),
    prices as ({{ get_coingecko_price_with_latest("akash-network") }})
select f.date, f.validator_fees_native * p.price as validator_fees
from fees f
left join prices p on f.date = p.date
