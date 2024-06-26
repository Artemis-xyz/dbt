-- depends_on {{ ref("ez_arbitrum_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}

{{ get_fundamental_data_for_chain_by_application("arbitrum") }}
