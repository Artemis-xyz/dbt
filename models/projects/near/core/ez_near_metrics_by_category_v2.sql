-- depends_on: {{ ref("ez_near_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("near") }}
