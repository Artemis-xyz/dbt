-- depends_on: {{ ref('fact_ethereum_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_MD",
    )
}}

{{ fact_chain_transactions_gold("ethereum") }}
