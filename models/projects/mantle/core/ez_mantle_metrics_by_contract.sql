-- depends_on {{ ref("ez_mantle_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="mantle",
        database="mantle",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("mantle") }}
