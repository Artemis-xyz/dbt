 {% macro across_v3_decode_filled_relay(chain, spot_fee_contract) %}       
        select
            contract_address as messaging_contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"depositId"::integer as deposit_id,
            decoded_log:"inputToken"::string as origin_token,
            decoded_log:"inputAmount"::double as src_amount,
            coalesce(decoded_log:"relayExecutionInfo":"updatedOutputAmount"::double, decoded_log:"outputAmount"::double) as dst_amount,
            decoded_log:"depositor"::string as depositor,
            coalesce(decoded_log:"relayExecutionInfo":"updatedRecipient"::string, decoded_log:"recipient"::string) as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            decoded_log:"outputToken"::string as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            null as realized_lp_fee_pct,
            null as relayer_fee_pct,
            src_amount - dst_amount as protocol_fee,
            decoded_log:"message"::string as message,
            '{{ chain }}' as chain,
            decoded_log
        from {{chain}}_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledV3Relay'
            and
            lower(contract_address) = lower('{{ spot_fee_contract }}')
        {% if is_incremental() %}

            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

        {% endif %}
{% endmacro %}

{% macro across_v3_goldsky_decode_filled_relay(chain, spot_fee_contract) %}       
        select
            contract_address as messaging_contract_address,
            block_timestamp,
            transaction_hash as tx_hash,
            event_index,
            decoded_log:"depositId"::integer as deposit_id,
            decoded_log:"inputToken"::string as origin_token,
            decoded_log:"inputAmount"::double as src_amount,
            decoded_log:"outputAmount"::double as dst_amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            decoded_log:"outputToken"::string as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            null as realized_lp_fee_pct,
            null as relayer_fee_pct,
            src_amount - dst_amount as protocol_fee,
            decoded_log:"message"::string as message,
            '{{ chain }}' as chain,
            decoded_log
        from {{ ref("fact_" ~ chain  ~ "_decoded_events") }}
        where
            event_name = 'FilledV3Relay'
            and
            lower(contract_address) = lower('{{ spot_fee_contract }}')
        {% if is_incremental() %}

            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

        {% endif %}
{% endmacro %}

{% macro across_v3_rpc_decode_filled_relay(chain) %}       
    with extraction_dates as (
        select
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            max(extraction_date) as extraction_date
        from
            {{ source("PROD_LANDING", "raw_across_v3_" ~ chain ~ "_filled_relay_events") }},
            lateral flatten(input => to_variant(source_json)) as flat_json
        {% if is_incremental() %}
            where
                date_trunc('day', flat_json.value:"block_timestamp"::timestamp) >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
        group by date
        order by date
    ),
    flattened_json as (
        select
            extraction_date,
            date_trunc('day', flat_json.value:"block_timestamp"::timestamp) as date,
            to_timestamp(flat_json.value:"block_timestamp"::timestamp) as block_timestamp,
            flat_json.value:"contract_address"::string as contract_address,
            flat_json.value:"tx_hash"::string as tx_hash,
            flat_json.value:"event_index"::integer as event_index,
            flat_json.value:"input_amount"::double as src_amount,
            flat_json.value:"output_amount"::double as dst_amount,
            flat_json.value:"deposit_id"::integer as deposit_id,
            flat_json.value:"depositor"::string as depositor,
            flat_json.value:"recipient"::string as recipient,
            flat_json.value:"destination_chain"::string as destination_chain,
            flat_json.value:"fill_deadline"::string as fill_deadline,
            flat_json.value:"exclusivity_deadline"::string as exclusivity_deadline,
            flat_json.value:"exclusive_relayer"::string as exclusive_relayer,
            flat_json.value:"relayer"::string as relayer,
            flat_json.value:"repayment_chain_id"::integer as repayment_chain_id,
            flat_json.value:"destination_token"::string as destination_token,
            flat_json.value:"origin_chain_id"::integer as origin_chain_id,
            flat_json.value:"input_token"::string as origin_token,
            flat_json.value as decoded_log
        from
            {{ source("PROD_LANDING", "raw_across_v3_" ~ chain ~ "_filled_relay_events") }},
            lateral flatten(input => to_variant(source_json)) as flat_json
        {% if is_incremental() %}
            where
                date_trunc('day', flat_json.value:"block_timestamp"::timestamp) >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
    select
        t1.contract_address as messaging_contract_address
        , t1.block_timestamp
        , t1.tx_hash
        , t1.event_index
        , t1.deposit_id
        , t1.origin_token
        , t1.src_amount
        , t1.dst_amount
        , t1.depositor
        , t1.recipient
        , null as destination_chain_id
        , t1.destination_token
        , t1.origin_chain_id
        , null as realized_lp_fee_pct
        , null as relayer_fee_pct
        , t1.src_amount - t1.dst_amount as protocol_fee
        , null as message
        , '{{ chain }}' as chain
        , t1.decoded_log
    from flattened_json t1
    left join
        extraction_dates t2
        on t1.date = t2.date
        and t1.extraction_date = t2.extraction_date
{% endmacro %}
