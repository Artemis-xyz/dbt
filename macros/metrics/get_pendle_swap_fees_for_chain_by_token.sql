{% macro get_pendle_swap_fees_for_chain_by_token(chain)%}

    with
        all_logs as(
            SELECT * FROM
            {{ chain }}_flipside.core.ez_decoded_event_logs
            WHERE event_name in ('Swap', 'CreateNewMarket', 'Deposit')
            {% if is_incremental() %}
                AND block_timestamp > (select max(date) from {{ this }})
            {% endif %}
        )

        , swap_logs as (
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
            FROM all_logs
            WHERE event_name = 'Swap'
            AND DECODED_LOG:netSyFee IS NOT NULL
            AND DECODED_LOG:netSyToReserve IS NOT NULL
        )
        -- Get all markets and PTs
        , pt_addresses as (
            SELECT
                tx_hash
                , block_timestamp
                , DECODED_LOG:PT::STRING as pt_address
                , DECODED_LOG:market::STRING as market_address
                , contract_address as market_factory_address
            FROM
                all_logs
            WHERE
                event_name = 'CreateNewMarket'
        )
        -- Get all PTs and SYs
        , sy_addresses as(
            SELECT
                tx_hash
                , block_timestamp
                , '0x' || SUBSTR(input, 35, 40) as sy_address
                , '0x' || SUBSTR(OUTPUT, 27, 40) as pt_address
                , *
            FROM
                {{ chain }}_flipside.core.fact_traces
            WHERE SUBSTR(input, 0, 10) = '0xe28a68b6'
            AND SUBSTR(input, 35, 40) is not null
            {% if is_incremental() %}
                AND block_timestamp > (select max(date) from {{ this }})
            {% endif %}
        )
        -- Get all SYs and underlyings
        , deposits as (
            SELECT
                distinct
                contract_address as sy
                , contract_name as sy_name
                , DECODED_LOG:tokenIn::STRING as underlying
                , count(*) as count
            FROM all_logs
            WHERE contract_address in (SELECT distinct(sy_address) FROM sy_addresses)
            AND event_name = 'Deposit'
            AND DECODED_LOG:tokenIn::STRING <> '0x0000000000000000000000000000000000000000'
            GROUP BY 1, 2, 3
        )
        -- Get distinct underlyings
        , underlyings as (
            SELECT
                sy as sy_address
                , sy_name
                , MAX_BY(underlying, count) as underlying
            FROM deposits
            GROUP BY 1, 2
        )
        -- Match market to underlying
        , market_to_underlying as(
            SELECT
                p.market_address
                , u.underlying
            FROM
                pt_addresses p
            LEFT JOIN sy_addresses s on p.pt_address = s.pt_address
            LEFT JOIN underlyings u on u.sy_address = s.sy_address
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
                , m.underlying
            FROM
                swap_logs l
            LEFT JOIN market_to_underlying m on m.market_address = l.market_address
            LEFT JOIN {{ chain }}_flipside.price.ez_prices_hourly p on p.hour = date_trunc('hour', l.block_timestamp) AND lower(p.token_address) = lower(m.underlying)        )
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