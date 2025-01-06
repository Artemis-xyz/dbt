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
        ),

        -- Identify new underlying addresses based on deposits related to new SYs
        deposits as (
            SELECT 
                contract_address as sy_address,
                '0x' || RIGHT(topics[3], 40) as underlying_address,
                count(*) as deposit_count
            FROM {{ chain }}_flipside.core.fact_event_logs
            WHERE 1=1
                AND '0x' || RIGHT(topics[3], 40) <> '0x0000000000000000000000000000000000000000' -- tokenIn cannot be 0x000...
                AND contract_address IN (SELECT sy_address FROM sy_addresses)
                AND topics[0] = '0x5fe47ed6d4225326d3303476197d782ded5a4e9c14f479dc9ec4992af4e85d59' -- Deposit event
                -- AND contract_address IN (lower('0x9d6Ec7a7B051B32205F74B140A0fa6f09D7F223E')) -- this is a contact address we know exists in our 
            GROUP BY
                1, 2
        ),

        -- Rank new underlying addresses by deposit count to get the most frequent one
        ranked_underlyings as (
            SELECT
                sy_address,
                underlying_address,
                ROW_NUMBER() OVER (PARTITION BY sy_address ORDER BY deposit_count DESC) as rank
            FROM
                deposits
        ),

        -- Filter to get only the top-ranked underlying address for each SY
        underlyings as (
            SELECT
                sy_address,
                underlying_address
            FROM
                ranked_underlyings
            WHERE
                rank = 1
        )

    -- Final query to combine all the information into the dim_pendle_markets_data table
    SELECT
        p.market_address,
        p.pt_address,
        s.sy_address,
        u.underlying_address,
        MAX(block_timestamp) as block_timestamp
    FROM
        pt_addresses p
    LEFT JOIN
        sy_addresses s ON p.pt_address = s.pt_address
    LEFT JOIN
        underlyings u ON s.sy_address = u.sy_address
    WHERE
        u.underlying_address IS NOT NULL
    GROUP BY
        p.market_address,
        p.pt_address,
        s.sy_address,
        u.underlying_address
{% endmacro %}