select v.date, v.validator_fees + c.compute_fees_total_usd / 1e6 as total_fees
from pc_dbt_db.prod.fact_akash_validator_fees_silver v
left join pc_dbt_db.prod.fact_akash_compute_fees_total_usd_silver c on v.date = c.date
order by date desc
