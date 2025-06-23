{% macro get_pendle_yield_contract_creation_events(chain) %}

    SELECT
        block_timestamp,
        '{{ chain }}' as chain,
        block_number,
        tx_hash,
        event_index,
        contract_address as factory_address,
        DECODED_LOG:SY::STRING as sy_address,
        TRY_TO_NUMBER(DECODED_LOG:expiry::STRING) as expiry_timestamp,
        to_timestamp(TRY_TO_NUMBER(DECODED_LOG:expiry::STRING)) as expiry_date,
        DECODED_LOG:PT::STRING as pt_address,
        DECODED_LOG:YT::STRING as yt_address
    FROM {{ chain }}_flipside.core.ez_decoded_event_logs
    WHERE event_name = 'CreateYieldContract'
    AND DECODED_LOG:SY IS NOT NULL
    AND DECODED_LOG:PT IS NOT NULL
    AND DECODED_LOG:YT IS NOT NULL
    AND DECODED_LOG:expiry IS NOT NULL
    {% if is_incremental() %}
        AND block_timestamp > (select max(block_timestamp) from {{ this }})
    {% endif %}
    ORDER BY block_timestamp DESC

{% endmacro %}