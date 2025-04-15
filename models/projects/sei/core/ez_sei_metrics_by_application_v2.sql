-- depends_on {{ ref("fact_sei_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="sei_lg",
        database="sei",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("sei", "v2") }}
