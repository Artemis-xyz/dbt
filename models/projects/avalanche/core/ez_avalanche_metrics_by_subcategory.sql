-- depends_on {{ ref("fact_avalanche_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE_MD",
        database="avalanche",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("avalanche", "v2") }}
