-- depends_on {{ ref("fact_base_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="base_md",
        database="base",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("base", "v2") }}
