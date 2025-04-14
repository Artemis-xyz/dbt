-- depends_on {{ ref("fact_tron_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="tron_lg",
        database="tron",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("tron", "v2") }}
