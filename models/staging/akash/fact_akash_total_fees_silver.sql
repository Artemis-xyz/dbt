{{ config(snowflake_warehouse="AKASH") }}

select v.date, v.validator_fees + c.compute_fees_total_usd / 1e6 as total_fees
from {{ref("fact_akash_validator_fees_silver")}} v
left join {{ref("fact_akash_compute_fees_total_usd_silver")}} c on v.date = c.date
order by date desc
