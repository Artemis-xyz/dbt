{{
    config(
        materialized="table",
        snowflake_warehouse="SOLANA_XLG",
        database="mantle",
        schema="raw",
        alias="fact_mantle_owned_balances",
    )
}}


{{get_entity_historical_balance(
    chain='mantle',
    table_name='dim_mantle_owned_addresses',
    address_column='address',
    earliest_date='2023-11-27'
)}}
