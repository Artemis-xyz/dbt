-- depends_on {{ ref("fact_optimism_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="optimism_md",
        database="optimism",
        schema="core",
        alias="ez_metrics_by_application_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_application("optimism", "v2") }}
