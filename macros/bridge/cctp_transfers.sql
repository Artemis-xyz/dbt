{% macro cctp_transfers(chain, contract_address, source_domain_id) %}
    {% if chain == 'solana' %}
        select
            block_timestamp,
            block_id as block_number,
            tx_id as tx_hash,
            program_id as contract_address,
            PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data) as hex_data,
            PC_DBT_DB.PROD.hex_to_base58(SUBSTRING(hex_data,129,64)) as sender, -- Need to figure out which one is the sender
            PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,33,16)) as nonce,
            '0x' || SUBSTRING(hex_data,193,64) as reciepient,
            PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,113,16)) as amount,
            PC_DBT_DB.PROD.hex_to_base58(SUBSTRING(hex_data,49,64)) as burn_token,
            5 as source_domain_id,
            PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,257,8)) as destination_domain_id
            succeeded as status
        FROM solana_flipside.core.fact_events,
        lateral flatten(input => get_path(inner_instruction, 'instructions')) AS f
        where program_id = 'CCTPiPYPc6AsJuwueEnWgSgucamXDZwBd53dQ11YiKX3'
        and SUBSTRING(hex_data,17,16) = '90fc9192064aa7eb'
    {% else %}
        select 
            block_timestamp,
            block_number,
            tx_hash,
            contract_address,
            decoded_log:depositor::string as sender,
            decoded_log:amount::number as nonce,
            decoded_log:mintRecipient::string as reciepient,
            decoded_log:amount::number as amount,
            decoded_log:burnToken::string as burn_token,
            {{ source_domain_id }} as source_domain_id,
            decoded_log:destinationDomain::number as destination_domain_id ,
            tx_status as status
        from {{ chain }}_flipside.core.ez_decoded_event_logs 
        where lower(contract_address) = lower('{{ contract_address }}') and event_name in ('DepositForBurn')
    {% endif %}
{% endmacro %}