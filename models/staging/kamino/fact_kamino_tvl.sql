{{ config(materialized="table", snowflake_warehouse="KAMINO") }}

{{get_entity_historical_balance(
    chain='solana',
    table_name='dim_kamino_reserve_addresses',
    address_column='reserve_address',
    earliest_date='2023-11-01'
)}}
    