 {% macro across_v2_decode_filled_relay(chain, spot_fee_contract) %}       
        select
            contract_address as messaging_contract_address,
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"depositId"::integer as deposit_id,
            decoded_log:"fillAmount"::double as amount,
            decoded_log:"depositor"::string as depositor,
            decoded_log:"recipient"::string as recipient,
            decoded_log:"destinationChainId"::integer as destination_chain_id,
            decoded_log:"destinationToken"::string as destination_token,
            decoded_log:"originChainId"::integer as origin_chain_id,
            decoded_log:"realizedLpFeePct"::integer / 1e18 as realized_lp_fee_pct,
            decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct,
            decoded_log:"message"::string as message,
            '{{ chain }}' as chain
        from {{chain}}_flipside.core.fact_decoded_event_logs
        where
            event_name = 'FilledRelay'
            and
            contract_address = '{{ spot_fee_contract }}'
            {% if is_incremental() %}

                and block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})

            {% endif %}
{% endmacro %}