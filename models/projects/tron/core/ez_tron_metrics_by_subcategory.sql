-- depends_on {{ ref("ez_tron_transactions") }}
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
