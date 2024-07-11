{% macro chainlink_vrf_request_fulfilled_logs(chain) %}
with
    v1_fulfilled_logs as (
        select
            block_timestamp,
            tx_hash,
            decoded_log:"requestId" as request_id
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where topics[0]::string = '0xa2e7a402243ebda4a69ceeb3dfb682943b7a9b3ac66d6eefa8db65894009611c'
        {% if is_incremental() %}
            and block_timestamp >= (select max(block_timestamp) from {{ this }})
        {% endif %}
    )
    , v1_random_request_logs as (
        select
            block_timestamp,
            tx_hash,
            origin_from_address as tx_from,
            decoded_log:"fee"::double as fee,
            decoded_log:"requestID" as request_id
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where topics[0]::string = '0x56bd374744a66d531874338def36c906e3a6cf31176eb1e9afd9f1de69725d51'
        {% if is_incremental() %}
            and block_timestamp >= (select max(block_timestamp) from {{ this }})
        {% endif %}
    )
    , v2_random_fulfilled_logs as (
        select
            block_timestamp,
            tx_hash,
            event_index,
            decoded_log:"payment" as payment
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where topics[0]::string = '0x7dffc5ae5ee4e2e4df1651cf6ad329a73cebdb728f37ea0187b9b17e036756e4'
        {% if is_incremental() %}
            and block_timestamp >= (select max(block_timestamp) from {{ this }})
        {% endif %}
    )
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
{% endmacro %}