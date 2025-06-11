-- depends_on: {{ ref('fact_near_transactions_v2') }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("near", "v2") }}
