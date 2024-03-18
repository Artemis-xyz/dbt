-- depends_on {{ ref("ez_avalanche_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}

{{ get_fundamental_data_for_chain_by_application("avalanche") }}
