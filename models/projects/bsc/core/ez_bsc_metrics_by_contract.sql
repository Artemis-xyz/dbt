-- depends_on {{ ref("ez_bsc_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BSC_SM",
        database="bsc",
        schema="core",
        alias="ez_metrics_by_contract",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("bsc") }}
