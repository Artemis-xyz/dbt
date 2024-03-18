-- depends_on {{ ref("ez_optimism_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="optimism",
        database="optimism",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("optimism") }}
