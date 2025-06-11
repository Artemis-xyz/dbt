{% macro cctp_transfers(chain, contract_address, source_domain_id) %}
    {% if chain == 'solana' %}
        select
            block_timestamp,
            block_id as block_number,
            tx_id as tx_hash,
            '{{ contract_address }}' as contract_address,
            PC_DBT_DB.PROD.hex_to_base58(SUBSTRING(PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data),129,64)) as sender,
            PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data),33,16)) as nonce,
            '0x' || SUBSTRING(PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data),193,64) as reciepient,
            PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data),113,16)) as amount,
            PC_DBT_DB.PROD.hex_to_base58(SUBSTRING(PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data),49,64)) as burn_token,
            {{ source_domain_id }} as source_domain_id,
            PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data),257,8)) as destination_domain_id,
        from solana_flipside.core.fact_events,
        lateral flatten(input => get_path(inner_instruction, 'instructions')) AS f
        where program_id = '{{ contract_address }}'
        and SUBSTRING(PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data),17,16) = '90fc9192064aa7eb'
        and succeeded = 'TRUE'
        {% if is_incremental() %}
            and block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }} where source_domain_id = {{ source_domain_id }})
        {% endif %}
    {% elif chain == 'noble' %}
        select 
            source:block_timestamp::timestamp as block_timestamp,
            source:block_number::number as block_number,
            source:tx_hash::string as tx_hash,
            '{{ contract_address }}' as contract_address,
            source:message:"from"::string as sender,
            null as nonce,
            source:message:"mint_recipient"::string as reciepient,
            source:message:"amount"::number as amount,
            source:message:"burn_token"::string as burn_token,
            {{ source_domain_id }} as source_domain_id,
            source:message:"destination_domain"::number as destination_domain_id
        from (
            select 
                max_by(value, extraction_date) as source
            from landing_database.prod_landing.raw_noble_cctp_messages,
            lateral flatten(input => parse_json(source_json))
            group by value
        )
        where source:message:"burn_token"::string != '' -- bug in noble chain data
        {% if is_incremental() %}
            and source:block_timestamp::timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }} where source_domain_id = {{ source_domain_id }})
        {% endif %} 
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
            decoded_log:destinationDomain::number as destination_domain_id
        from {{ chain }}_flipside.core.ez_decoded_event_logs 
        where lower(contract_address) = lower('{{ contract_address }}') and event_name in ('DepositForBurn')
        and tx_succeeded = TRUE
        {% if is_incremental() %}
            and block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }} where source_domain_id = {{ source_domain_id }})
        {% endif %}
    {% endif %}
{% endmacro %}