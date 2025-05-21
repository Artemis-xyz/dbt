{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

with all_balances as (
    {{ forward_filled_token_balances('ethereum', 'native_token') }}
)
select
    date,
    sum(balance_native) as balance_native,
    sum(balance) as balance
from all_balances
GROUP BY 1