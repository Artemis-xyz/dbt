{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_3XLG"
    )
}}

-- Sample run: dbt run -s fact_solana_address_balances_forward_filled_2025_h2 --vars '{"start_date": "2025-07-01", "end_date": "2025-08-01"}'

{% set start_date = var('start_date', '') %}
{% set end_date = var('end_date', '') %}

{% set max_date = '2025-12-31' %}
{% set min_date = '2025-07-01' %}

{{ solana_address_balances_forward_filled(start_date, end_date, max_date, min_date) }}