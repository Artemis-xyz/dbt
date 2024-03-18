-- depends_on {{ ref("ez_tron_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="tron",
        database="tron",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("tron") }}
