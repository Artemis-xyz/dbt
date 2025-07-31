{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL"
    )
}}

with treasury_balance as (
    with address_1 as (
        {{ forward_filled_balance_for_address('ethereum', '0xB6E39e540e3472E927001bbBf77FFB5AB357b0e0') }}
    )
    , address_2 as (
        {{ forward_filled_balance_for_address('ethereum', '0xe9f57628059ac658901ce42a83fe09272a62a6c8') }}
    )
    , address_3 as (
        {{ forward_filled_balance_for_address('ethereum', '0x9ad51cffff91a24b352c11293bdb8810f0da137d') }}
    )
    SELECT * FROM address_1
    UNION ALL
    SELECT * FROM address_2
    UNION ALL
    SELECT * FROM address_3
)
SELECT
    *
FROM treasury_balance