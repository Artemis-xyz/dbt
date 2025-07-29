{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_3XLG"
    )
}}

-- Sample run: dbt run -s fact_solana_address_balances_forward_filled_2024_h1 --vars '{"start_date": "2024-01-01", "end_date": "2024-02-01"}'

{% set start_date = var('start_date', '') %}
{% set end_date = var('end_date', '') %}

{% set max_date = '2024-06-30' %}
{% set min_date = '2024-01-01' %}

{{ solana_address_balances_forward_filled(start_date, end_date, max_date, min_date) }}