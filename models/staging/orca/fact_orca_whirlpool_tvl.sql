{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
    )
}}

{{get_entity_historical_balance(
    chain = 'solana',
    table_name = 'fact_orca_whirlpool_pubkeys',
    address_column = 'token_vault_pubkey',
    earliest_date = '2022-03-23'
)}}