-- depends_on {{ ref("fact_ethereum_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM_LG",
        database="ethereum",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("ethereum", "v2") }}
