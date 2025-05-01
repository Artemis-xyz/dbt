{{
    config(
        materialized="table",
        snowflake_warehouse="CENTRIFUGE",
    )
}}

{{ parse_parity_evm_transaction_parquets('centrifuge', 'centrifuge') }}
