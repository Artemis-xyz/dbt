{% macro get_pendle_yield_fees_for_chain_by_token(chain)%}

    with
    all_logs as (
        SELECT * FROM {{ chain }}_flipside.core.ez_decoded_event_logs
        WHERE event_name in ('CreateYieldContract', 'CollectInterestFee', 'Deposit')
        {% if is_incremental() %}
            AND block_timestamp > (select max(date) from {{ this }})
        {% endif %}
    )
    , yt_addresses as (
        SELECT
            DECODED_LOG:YT::STRING as yt_address
            , DECODED_LOG:SY::STRING as sy_address
        FROM
            all_logs
        WHERE event_name = 'CreateYieldContract'
    )
    , fees as (
        SELECT
            block_timestamp
            , tx_hash
            , contract_address as yt_address
            , yt.sy_address
            , decoded_log:amountInterestFee::number /1e18 as fee_sy_amount
        FROM
        all_logs l
        LEFT JOIN yt_addresses yt ON yt.yt_address = l.contract_address
        WHERE event_name = 'CollectInterestFee'
        and contract_address in (SELECT distinct yt_address FROM yt_addresses)
    )
    , deposits as (
        SELECT
            distinct
            contract_address as sy
            , contract_name as sy_name
            , DECODED_LOG:tokenIn::STRING as underlying
            , count(*) as count
        FROM all_logs
        WHERE contract_address in (SELECT distinct(sy_address) FROM yt_addresses)
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
    SELECT
        date(block_timestamp) as date
        , tx_hash
        , underlying as symbol
        , fee_sy_amount * p.price as yield_fee_usd
        , fee_sy_amount as yield_fee_native
    FROM
        fees f
    LEFT JOIN underlyings u ON f.sy_address = u.sy_address
    LEFT JOIN {{ chain }}_flipside.price.ez_prices_hourly p ON p.hour = date_trunc('hour', f.block_timestamp) AND lower(p.token_address) = lower(u.underlying)

{% endmacro %}