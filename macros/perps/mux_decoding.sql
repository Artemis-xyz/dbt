{% macro mux_decoding(chain, liquidity_contract) %}

    select
        block_timestamp,
        tx_hash,
        case
            when decoded_log:"args":"amount"::integer is null
            then
                (
                    (decoded_log:"args"[3] / pow(10, 18))
                    * (decoded_log:"args"[4] / pow(10, 18))
                )
            else
                (
                    (decoded_log:"args":"amount"::integer / pow(10, 18))
                    * (decoded_log:"args":"assetPrice"::integer / pow(10, 18))
                )
        end as trading_volume,
        decoded_log:"trader"::string as trader,
        '{{ chain }}' as chain
    from {{ chain }}_flipside.core.ez_decoded_event_logs
    where
        contract_address = lower('{{ liquidity_contract }}')
        and (event_name in ('ClosePosition', 'OpenPosition'))
        {% if is_incremental() %}
            and block_timestamp >= (
                select dateadd('day', -4, max(block_timestamp))
                from {{ this }}
                where chain = '{{ chain }}'
            )
        {% endif %}

{% endmacro %}
