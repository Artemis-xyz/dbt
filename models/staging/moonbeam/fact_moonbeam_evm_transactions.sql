{{
    config(
        materialized="table",
        snowflake_warehouse="MOONBEAM",
    )
}}

{{ parse_parity_evm_transaction_parquets('moonbeam', 'moonbeam') }}
