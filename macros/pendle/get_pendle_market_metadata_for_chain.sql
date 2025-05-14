{% macro get_pendle_markets_for_chain(chain) %}

    with
        -- Filter to get only new 'CreateNewMarket' logs after the last run date
        pt_addresses as (
            SELECT
                DECODED_LOG:PT::STRING as pt_address,
                DECODED_LOG:market::STRING as market_address,
                block_timestamp
            FROM
                {{ chain }}_flipside.core.ez_decoded_event_logs
            WHERE
                event_name = 'CreateNewMarket'
                {% if is_incremental() %}
                    AND block_timestamp > dateadd(day, -3, to_date(sysdate()))
                {% else %}
                    AND block_timestamp > '2022-11-22'
                {% endif %}
        ),

        -- Filter to get new SY addresses related to new PTs
        sy_addresses as (
            SELECT
                '0x' || SUBSTR(input, 35, 40) as sy_address,
                '0x' || SUBSTR(OUTPUT, 27, 40) as pt_address
            FROM
                {{ chain }}_flipside.core.fact_traces
            WHERE
                SUBSTR(input, 0, 10) = '0xe28a68b6'
                AND SUBSTR(input, 35, 40) is not null
                AND pt_address IN (SELECT pt_address FROM pt_addresses)
                {% if is_incremental() %}
                    AND block_timestamp > dateadd(day, -3, to_date(sysdate()))
                {% else %}
                    AND block_timestamp > '2022-11-22'
                {% endif %}
        )
    -- Final query to combine all the information into the dim_pendle_markets_data table
    SELECT
        p.market_address,
        p.pt_address,
        s.sy_address,
        MAX(block_timestamp) as block_timestamp
    FROM
        pt_addresses p
    LEFT JOIN
        sy_addresses s ON p.pt_address = s.pt_address
    GROUP BY
        p.market_address,
        p.pt_address,
        s.sy_address
{% endmacro %}