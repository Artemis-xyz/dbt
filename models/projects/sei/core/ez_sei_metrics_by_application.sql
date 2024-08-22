-- depends_on {{ ref("ez_sei_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="sei",
        database="sei",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}

{{ get_fundamental_data_for_chain_by_application("sei") }}
