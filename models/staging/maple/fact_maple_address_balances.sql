{{ config(materialized="table", snowflake_warehouse="MAPLE") }}

{{forward_filled_address_balances('ethereum', 'maple', 'treasury')}}