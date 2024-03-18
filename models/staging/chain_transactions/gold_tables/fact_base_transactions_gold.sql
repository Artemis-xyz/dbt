-- depends_on: {{ ref('fact_base_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="BAM_TRANSACTION_SM",
    )
}}

{{ fact_chain_transactions_gold("base") }}
