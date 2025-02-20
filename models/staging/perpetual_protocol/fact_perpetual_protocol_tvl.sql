{{ config(
    materialized="table"
) }}

with optimism_balances as (
    {{
        get_treasury_balance(
            chain='optimism',
            addresses='0xAD7b4C162707E0B2b5f6fdDbD3f8538A5fbA0d60',
            earliest_date='2021-12-19'
        )
    }}
)
select
    date,
    chain,
    sum(usd_balance) as tvl
from optimism_balances
group by date, chain
