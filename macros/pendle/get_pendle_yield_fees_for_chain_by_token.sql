{% macro get_pendle_yield_fees_for_chain_by_token(chain)%}

    with
        yt_addresses as (
            SELECT
                DECODED_LOG:YT::STRING as yt_address
                , DECODED_LOG:SY::STRING as sy_address
            FROM
                {{ chain }}_flipside.core.ez_decoded_event_logs
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
            {{ chain }}_flipside.core.ez_decoded_event_logs l
            LEFT JOIN yt_addresses yt ON yt.yt_address = l.contract_address
            WHERE event_name = 'CollectInterestFee'
            and contract_address in (SELECT distinct yt_address FROM yt_addresses)
            {% if is_incremental() %}
                AND block_timestamp > (select max(date) from {{ this }})
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
        SELECT
            distinct
            date(block_timestamp) as date
            , tx_hash
            , underlying_address as token_address
            , p.symbol as token
            , fee_sy_amount * p.price as yield_fee_usd
            , fee_sy_amount as yield_fee_native
        FROM
            fees f
        LEFT JOIN market_metadata m ON f.sy_address = m.sy_address
        LEFT JOIN {{ chain }}_flipside.price.ez_prices_hourly p ON p.hour = date_trunc('hour', f.block_timestamp) AND lower(p.token_address) = lower(m.underlying_address)
        WHERE fee_sy_amount * p.price < 1e7 -- Less than 10M USD

{% endmacro %}