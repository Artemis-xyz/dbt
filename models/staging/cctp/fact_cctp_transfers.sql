with
ethereum_source_transfers as (
    {{ cctp_evm_transfers("ethereum", "0xbd3fa81b58ba92a82136038b25adec7066af3155", 0) }}
),
avalanche_source_transfers as (
    {{ cctp_evm_transfers("avalanche", "0x6b25532e1060ce10cc3b0a99e5683b91bfde6982", 1) }}
),
optimism_source_transfers as (
    {{ cctp_evm_transfers("optimism", "0x2B4069517957735bE00ceE0fadAE88a26365528f", 2) }}
),
arbitrum_source_transfers as (
    {{ cctp_evm_transfers("arbitrum", "0x19330d10D9Cc8751218eaf51E8885D058642E08A", 3) }}
),
base_source_transfers as (
    {{ cctp_evm_transfers("base", "	0x1682Ae6375C4E4A97e4B583BC394c861A46D8962", 6) }}
),
polygon_source_transfers as (
    {{ cctp_evm_transfers("polygon", "0x9daF8c91AEFAE50b9c0E69629D3F6Ca40cA3B3FE", 7) }}
),
solana_source_transfers as (
    {{ cctp_solana_transfers("solana", "CCTPiPYPc6AsJuwueEnWgSgucamXDZwBd53dQ11YiKX3", 4) }}
),
noble_source_transfers as (
    {{ cctp_noble_transfers("noble", "noble1afmt2kk6n9fr7pwkjhe25hz86dlmkp2v4phl98", 5) }}
)



-- select 
--   block_timestamp,
--   block_height as block_number,
--   tx_id as tx_hash,
--   'noble1afmt2kk6n9fr7pwkjhe25hz86dlmkp2v4phl98' as contract_address,
--   JSON_EXTRACT(message, '$.from') as sender,
--   null as nonce,
--   JSON_EXTRACT(message, '$.mint_recipient') as reciepient,
--   JSON_EXTRACT(message, '$.amount') as amount,
--   JSON_EXTRACT(message, '$.burn_token') as burn_token,
--   4 as source_domain_id,
--   JSON_EXTRACT(message, '$.destination_domain') as destination_domain_id
-- from `numia-data.noble.noble_tx_messages` 
-- where SUBSTR(message_type, -17) = 'MsgDepositForBurn'
-- limit 100
