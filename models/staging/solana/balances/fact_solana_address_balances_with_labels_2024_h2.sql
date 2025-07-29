{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_2XLG"
    )
}}

{% set start_date = var('start_date', '') %}
{% set end_date = var('end_date', '') %}

{% set max_date = '2024-12-31' %}
{% set min_date = '2024-07-01' %}

-- Sample run: dbt run -s fact_solana_address_balances_with_labels_2024_h2 --vars '{"start_date": "2024-07-01", "end_date": "2024-08-01"}'

{{ solana_address_balances_with_labels(start_date, end_date, max_date, min_date) }}
