-- depends_on {{ ref("ez_ethereum_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("ethereum") }}
