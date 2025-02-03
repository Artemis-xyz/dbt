-- depends_on {{ ref("ez_avalanche_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE_LG",
        database="avalanche",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("avalanche", "v2") }}
