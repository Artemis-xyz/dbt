-- depends_on {{ ref("fact_celo_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="bam_transaction_sm",
        database="celo",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_contract("celo", "v2") }}
