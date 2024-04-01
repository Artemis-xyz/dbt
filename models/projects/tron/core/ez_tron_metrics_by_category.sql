-- depends_on: {{ ref('ez_tron_transactions') }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="TRON_LG",
        database="tron",
        schema="core",
        alias="ez_metrics_by_category",
    )
}}

{{ get_fundamental_data_for_chain_by_category("tron") }}
