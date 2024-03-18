-- depends_on: {{ ref('fact_avalanche_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="AVALANCHE",
    )
}}

{{ fact_chain_transactions_gold("avalanche") }}
