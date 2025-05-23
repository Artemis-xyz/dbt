{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

with all_balances as (
    {{ forward_filled_token_balances('solana', 'native_token', '2020-11-01') }}
)
select
    date,
    sum(balance_native) as balance_native,
    sum(balance) as balance
from all_balances
GROUP BY 1