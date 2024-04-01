-- depends_on {{ ref("ez_avalanche_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("avalanche") }}
