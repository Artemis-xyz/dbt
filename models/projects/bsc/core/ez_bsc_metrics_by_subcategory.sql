-- depends_on {{ ref("fact_bsc_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BSC",
        database="bsc",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("bsc", "v2") }}
