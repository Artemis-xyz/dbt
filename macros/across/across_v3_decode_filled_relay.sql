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