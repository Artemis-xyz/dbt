-- depends_on: {{ ref("fact_tron_transactions_v2") }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="TRON_LG",
        database="tron",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("tron", "v2") }}
