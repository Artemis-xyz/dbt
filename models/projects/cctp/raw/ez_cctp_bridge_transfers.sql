{{
    config(
        materialized="view",
        snowflake_warehouse="CCTP",
        database="cctp",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select
    block_timestamp,
    block_number,
    tx_hash as source_tx_hash,
    nonce as source_nonce,
    contract_address as source_contract_address,
    sender as source_address,
    c1.chain as source_chain,
    burn_token as source_token_address,
    reciepient as destination_address,
    c2.chain as destination_chain,
    amount,
    amount_usd,
from {{ref('fact_cctp_transfers')}} t
left join {{ref('cctp_chain_ids')}} c1 on t.source_domain_id = c1.chain_id
left join {{ref('cctp_chain_ids')}} c2 on t.destination_domain_id = c2.chain_id