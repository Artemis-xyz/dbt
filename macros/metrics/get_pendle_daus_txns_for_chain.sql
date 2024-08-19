{% macro get_pendle_daus_txns_for_chain(chain)%}

    with
        swap_logs as (
            SELECT
                block_timestamp,
                tx_hash,
                DECODED_LOG:caller::STRING AS caller,
                TRY_TO_NUMBER(DECODED_LOG:netPtOut::STRING) / 1e18 AS netPtOut,
                TRY_TO_NUMBER(DECODED_LOG:netSyFee::STRING) / 1e18 AS netSyFee,
                TRY_TO_NUMBER(DECODED_LOG:netSyOut::STRING) / 1e18 AS netSyOut,
                TRY_TO_NUMBER(DECODED_LOG:netSyToReserve::STRING) / 1e18 AS netSyToReserve,
                DECODED_LOG:receiver::STRING AS receiver
                , contract_address as market_address
                , origin_from_address as user
            FROM {{ chain }}_flipside.core.ez_decoded_event_logs
            WHERE event_name = 'Swap'
            {% if is_incremental() %}
                AND block_timestamp > (select max(date) from {{ this }})
            {% endif %}
            AND DECODED_LOG:netSyFee IS NOT NULL
            AND DECODED_LOG:netSyToReserve IS NOT NULL
        )
    SELECT
        date(block_timestamp) as date
        , '{{ chain }}' as chain
        , count(distinct user) as DAU
        , count(distinct tx_hash) as daily_txns
    FROM swap_logs
    GROUP BY 1

{% endmacro %}