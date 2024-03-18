-- depends_on {{ ref("ez_arbitrum_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("arbitrum") }}
