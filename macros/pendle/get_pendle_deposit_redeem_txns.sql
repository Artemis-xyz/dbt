{% macro get_pendle_deposit_redeem_txns(chain) %}

    WITH sy_addresses AS (
        SELECT sy_address
        FROM {{ref('dim_pendle_' ~ chain ~ '_market_metadata')}}
    )
    , decoded_events as (
        SELECT
            *
        FROM ethereum_flipside.core.ez_decoded_event_logs
        WHERE contract_address IN (SELECT DISTINCT(sy_address) FROM sy_addresses)
        AND event_name IN ('Deposit', 'Redeem')
        {% if is_incremental() %}
            AND block_timestamp > (SELECT DATEADD(day, -1, MAX(block_timestamp)) FROM {{this}})
        {% endif %}
    )
    SELECT
        DECODED_LOG:tokenIn::STRING as token_address,
        DECODED_LOG:amountDeposited::number as amount,
        contract_address as sy_address,
        *
    FROM decoded_events
    WHERE event_name = 'Deposit'
    UNION ALL
    SELECT
        DECODED_LOG:tokenOut::STRING as token_address,
        - DECODED_LOG:amountTokenOut::number as amount,
        contract_address as sy_address,
        *
    FROM decoded_events
    WHERE event_name = 'Redeem'


{% endmacro %}