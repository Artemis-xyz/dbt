{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_2XLG"
    )
}}

{% set start_date = var('start_date', '') %}
{% set end_date = var('end_date', '') %}

{% set max_date = '2025-06-30' %}
{% set min_date = '2025-01-01' %}

-- Sample run: dbt run -s fact_solana_address_balances_with_labels_2025_h1 --vars '{"start_date": "2025-01-01", "end_date": "2025-02-01"}'

{{ solana_address_balances_with_labels(start_date, end_date, max_date, min_date) }}
