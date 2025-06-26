{% macro get_pendle_daus_txns_for_chain(chain)%}

    with
        swap_logs as (
            SELECT
                block_timestamp,
                tx_hash,
                origin_from_address as user
            FROM {{ chain }}_flipside.core.ez_decoded_event_logs
            WHERE event_name = 'Swap'
            {% if is_incremental() %}
                AND block_timestamp > (select max(date) from {{ this }})
            {% endif %}
            AND DECODED_LOG:netSyFee IS NOT NULL
            AND DECODED_LOG:netSyToReserve IS NOT NULL

            UNION ALL
            SELECT
                block_timestamp,
                tx_hash,
                origin_from_address as user
            FROM {{ chain }}_flipside.core.ez_decoded_event_logs
            WHERE event_name = 'OrderFilledV2'
            {% if is_incremental() %}
                AND block_timestamp > (select max(date) from {{ this }})
            {% endif %}
            AND lower(contract_address) = lower('{{ router_address }}')
            AND DECODED_LOG:notionalVolume IS NOT NULL
        )
    SELECT
        date(block_timestamp) as date
        , '{{ chain }}' as chain
        , count(distinct user) as DAU
        , count(distinct tx_hash) as daily_txns
    FROM swap_logs
    GROUP BY 1

{% endmacro %}