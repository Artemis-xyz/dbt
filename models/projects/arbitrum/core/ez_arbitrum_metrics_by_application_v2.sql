-- depends_on {{ ref("fact_arbitrum_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM_MD",
        database="arbitrum",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("arbitrum", "v2") }}
