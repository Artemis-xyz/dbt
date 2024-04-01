-- depends_on: {{ ref('ez_near_transactions') }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}

{{ get_fundamental_data_for_chain_by_application("near") }}
