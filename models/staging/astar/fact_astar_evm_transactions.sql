{{
    config(
        materialized="table",
        snowflake_warehouse="ASTAR",
    )
}}

{{ parse_parity_evm_transaction_parquets('astar', 'astar') }}
