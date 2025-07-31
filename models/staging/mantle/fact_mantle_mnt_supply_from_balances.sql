{{
    config(
        materialized="table",
        snowflake_warehouse="MANTLE",
    )
}}

with all_balances as (
    {{ forward_filled_token_balances('ethereum', '0x3c3a81e81dc49A522A592e7622A7E711c06bf354') }}
)   
select
    date,
    address,
    sum(balance_native) as balance_native,
    sum(balance) as balance
from all_balances
WHERE balance_native > 0
GROUP BY 1, 2