-- depends_on {{ ref("fact_optimism_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="optimism_md",
        database="optimism",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("optimism", "v2") }}
