-- depends_on {{ ref("ez_near_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="NEAR",
        database="near",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("near") }}
