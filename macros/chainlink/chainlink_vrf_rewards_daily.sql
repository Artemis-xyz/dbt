{% macro chainlink_vrf_rewards_daily(chain) %}
with
    v1_fulfilled_logs as (
        select
            block_timestamp,
            tx_hash,
            decoded_log:"requestId" as request_id
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where event_name = 'RandomnessRequestFulfilled'
            and contract_address = '0xf0d54349addcf704f77ae15b96510dea15cb7952'
    )
    , v1_random_request_logs as (
        select
            block_timestamp,
            tx_hash,
            origin_from_address as tx_from,
            decoded_log:"fee"::double as fee,
            decoded_log:"requestID" as request_id
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where event_name = 'RandomnessRequest'
            and contract_address = '0xf0d54349addcf704f77ae15b96510dea15cb7952'
    )
    , v2_random_fulfilled_logs as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"payment" as payment
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where event_name = 'RandomWordsFulfilled'
            and contract_address = '0x271682deb8c4e0901d1a1550ad2e64d568e69909'
    )
    , logs as (
        SELECT
            MAX(CAST(v1_request.fee AS double))/1e18 AS token_value,
            MAX(v1_fulfilled_logs.block_timestamp) as evt_block_time
        FROM
            v1_random_request_logs v1_request
            INNER JOIN v1_fulfilled_logs
                ON v1_fulfilled_logs.request_id = v1_request.request_id
            GROUP BY
                v1_request.tx_hash
        UNION

        SELECT
            MAX(CAST(v2_random_fulfilled_logs.payment as double))/1e18 AS token_value,
            MAX(v2_random_fulfilled_logs.block_timestamp) as evt_block_time
        FROM
            v2_random_fulfilled_logs
            GROUP BY
                v2_random_fulfilled_logs.tx_hash,
                v2_random_fulfilled_logs.event_index
    )
    , vrf_daily as (
        SELECT
            cast(date_trunc('day', evt_block_time) AS date) AS date_start,
            SUM(token_value) as token_amount
        FROM logs 
        GROUP BY 1
    )
    , link_usd_daily AS ({{get_coingecko_price_with_latest("chainlink")}})
    , vrf_reward_daily AS (
        SELECT
            vrf_daily.date_start,
            COALESCE(vrf_daily.token_amount, 0) as token_amount,
            COALESCE(vrf_daily.token_amount * lud.price, 0)  as usd_amount
        FROM vrf_daily
        LEFT JOIN link_usd_daily lud ON lud.date = vrf_daily.date_start
        ORDER BY date_start
    )
    SELECT
        '{{chain}}' as blockchain
        , date_start as date
        , token_amount
        , usd_amount
    from vrf_reward_daily
    ORDER BY 2
{% endmacro %}