-- depends_on {{ ref("fact_near_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("near", "v2") }}
