{% macro across_v2_decode_funds_deposited(chain, spot_fee_contract) %}
    select
        decoded_log:"depositId"::integer as deposit_id,
        contract_address as messaging_contract_address,
        block_timestamp,
        tx_hash,
        event_index,
        decoded_log:"amount"::double as amount,
        decoded_log:"depositor"::string as depositor,
        decoded_log:"recipient"::string as recipient,
        decoded_log:"destinationChainId"::integer as destination_chain_id,
        decoded_log:"originChainId"::integer as origin_chain_id,
        decoded_log:"originToken"::string as origin_token,
        decoded_log:"relayerFeePct"::integer / 1e18 as relayer_fee_pct,
        '{{ chain }}' as chain
    from {{ chain }}_flipside.core.fact_decoded_event_logs
    where
        event_name = 'FundsDeposited'
        and
        contract_address = '{{ spot_fee_contract }}'
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
{% endmacro %}
