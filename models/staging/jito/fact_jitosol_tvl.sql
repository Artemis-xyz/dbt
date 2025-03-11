{{
    config(
        materialized="table",
        snowflake_warehouse="JITO"
    )
}}

{{get_entity_historical_balance(chain='solana', table_name='fact_jitosol_stake_accounts', address_column='stake_account', earliest_date='2022-10-24')}}