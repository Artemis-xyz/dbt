-- depends_on {{ ref("fact_tron_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="tron_lg",
        database="tron",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("tron", "v2") }}
