{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_owned_supply",
    )
}}


{{get_entity_historical_balance(
    chain='optimism',
    table_name='dim_optimism_owned_addresses',
    address_column='address',
    earliest_date='2022-04-26'
)}}
