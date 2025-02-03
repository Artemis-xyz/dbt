-- depends_on {{ ref("ez_arbitrum_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE_LG",
        database="avalanche",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("arbitrum") }}
