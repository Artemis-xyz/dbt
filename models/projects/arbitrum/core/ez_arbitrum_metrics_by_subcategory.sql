-- depends_on {{ ref("fact_arbitrum_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM_MD",
        database="arbitrum",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("arbitrum", "v2") }}
