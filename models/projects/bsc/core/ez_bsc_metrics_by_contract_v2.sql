-- depends_on {{ ref("ez_bsc_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BSC",
        database="bsc",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("bsc", "v2") }}
