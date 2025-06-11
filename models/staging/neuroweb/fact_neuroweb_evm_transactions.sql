{{
    config(
        materialized="table",
        snowflake_warehouse="neuroweb",
    )
}}

{{ parse_parity_evm_transaction_parquets('neuroweb', 'neurowebai') }}
