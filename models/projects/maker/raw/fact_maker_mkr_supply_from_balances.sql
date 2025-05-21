{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

with all_balances as (
    {{ forward_filled_token_balances('ethereum', '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2') }}
)
select
    date,
    address,
    sum(balance_native) as balance_native,
    sum(balance) as balance
from all_balances
WHERE balance_native > 0
GROUP BY 1, 2