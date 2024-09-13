{% macro get_pendle_swap_fees_for_chain_by_token(chain, blacklist=(''))%}

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
            FROM {{ chain }}_flipside.core.ez_decoded_event_logs
            WHERE event_name = 'Swap'
            AND DECODED_LOG:netSyFee IS NOT NULL
            AND DECODED_LOG:netSyToReserve IS NOT NULL
            {% if is_incremental() %}
                AND block_timestamp > (select max(date)-1 from {{ this }})
            {% endif %}
            {% if blacklist is string %} AND lower(contract_address) != '{{ blacklist }}'
            {% elif blacklist | length > 1 %} AND lower(contract_address) not in {{ blacklist }}
            {% endif %}
        )
        , market_metadata as (
            SELECT
                market_address,
                pt_address,
                sy_address,
                underlying_address
            FROM
                {{ ref("dim_pendle_" ~ chain ~ "_market_metadata") }}
        )
        , swaps_with_meta_data as (
            SELECT
                l.block_timestamp
                , p.symbol
                , p.price
                , l.netSyFee * p.price as fee_usd
                , l.netSyFee as fee_native
                , l.netSyOut * p.price as volume_usd
                , l.netSyOut as volume_native
                , l.netSyToReserve * p.price as revenue_usd
                , l.netSyToReserve as revenue_native
                , m.market_address
                , m.underlying_address
            FROM
                swap_logs l
            LEFT JOIN market_metadata m on m.market_address = l.market_address
            LEFT JOIN {{ chain }}_flipside.price.ez_prices_hourly p on p.hour = date_trunc('hour', l.block_timestamp) AND lower(p.token_address) = lower(m.underlying_address)
        )

    SELECT
        date(block_timestamp) as date
        , '{{ chain }}' as chain
        , symbol
        , SUM(fee_usd) as fee_usd
        , SUM(fee_native) as fee_native
        , SUM(volume_usd) as volume_usd
        , SUM(volume_native) as volume_native
        , SUM(revenue_usd) as revenue_usd
        , SUM(revenue_native) as revenue_native
    FROM swaps_with_meta_data
    GROUP BY 1, 2, 3

{% endmacro %}