{{
    config(
        materialized="table",
        snowflake_warehouse="HYDRATION",
    )
}}

{{ parse_parity_evm_transaction_parquets('hydration', 'ethereum') }}
