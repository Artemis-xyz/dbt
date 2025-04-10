-- depends_on {{ ref('fact_solana_transactions_v2') }}
{{
    config(
        materialized="table",
        unique_key="tx_hash",
        snowflake_warehouse="SOLANA_XLG",
        database="solana",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}

{{ get_fundamental_data_for_chain_by_category_v2("solana") }}