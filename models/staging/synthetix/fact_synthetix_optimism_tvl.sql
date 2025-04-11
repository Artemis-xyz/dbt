{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

{{forward_filled_address_balances('optimism', 'synthetix', 'pool')}}