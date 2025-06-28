{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
    )
}}

{{get_entity_historical_balance(
    chain='arbitrum',
    table_name='dim_arbitrum_owned_addresses',
    address_column='address',
    earliest_date='2023-03-23'
)}}