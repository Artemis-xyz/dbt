{{ config(
    materialized="table",
    snowflake_warehouse='MEDIUM',
) }}

with ethereum_balances as (
    {{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0x0f346e19F01471C02485DF1758cfd3d624E399B4',
        ],
        earliest_date='2021-06-28'
    )
}}
)
, optimism_balances as (
    {{ get_treasury_balance(
        chain='optimism',
        addresses=[
            '0xD360B73b19Fb20aC874633553Fb1007e9FcB2b78'
        ],
        earliest_date='2021-06-28'
    )
}}
)
, aggregate_balances as (
    select
        *
    from ethereum_balances
    where contract_address = lower('0xbC396689893D065F41bc2C6EcbeE5e0085233447')
    union all
    select
        *
    from optimism_balances
    where contract_address = lower('0x9e1028F5F1D5eDE59748FFceE5532509976840E0')
)
select
    date,
    chain,
    sum(usd_balance) as tvl
from aggregate_balances
group by date, chain
