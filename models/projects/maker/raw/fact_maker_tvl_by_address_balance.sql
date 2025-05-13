{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
        database="maker",
        schema="raw",
        alias="fact_maker_tvl"
    )
}}


WITH all_address_balances AS (
    {{ forward_filled_address_balances('ethereum', 'makerdao', 'lending_pool') }}
)
SELECT
    date
    , contract_address
    , address
    , balance
    , price
    , balance_native
FROM all_address_balances
WHERE contract_address not in (
    lower('0xdC035D45d973E3EC169d2276DDab16f1e407384F') -- USDS
    , lower('0x6B175474E89094C44Da98b954EedeAC495271d0F') -- DAI
    )
