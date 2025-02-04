-- depends_on {{ ref("ez_ethereum_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM_LG",
        database="ethereum",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}

{{ get_fundamental_data_for_chain_by_subcategory("ethereum") }}
