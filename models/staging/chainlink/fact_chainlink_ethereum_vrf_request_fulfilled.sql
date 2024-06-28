{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_vrf_request_fulfilled",
    )
}}


SELECT
    'ethereum' as blockchain,
    MAX(CAST(v1_request.fee AS double))/1e18 AS token_value,
    MAX(v1_fulfilled.block_timestamp) as evt_block_time
FROM
    {{ ref('fact_chainlink_ethereum_vrf_v1_random_request_logs') }} v1_request
    INNER JOIN {{ ref('fact_chainlink_ethereum_vrf_v1_random_fulfilled_logs') }} v1_fulfilled
    ON v1_fulfilled.request_id = v1_request.request_id
    GROUP BY
        v1_request.tx_hash
UNION

SELECT
    'ethereum' as blockchain,
    MAX(CAST(v2_fulfilled.payment as double))/1e18 AS token_value,
    MAX(v2_fulfilled.block_timestamp) as evt_block_time
FROM
    {{ ref('fact_chainlink_ethereum_vrf_v2_random_fulfilled_logs') }} v2_fulfilled
    GROUP BY
        v2_fulfilled.tx_hash,
        v2_fulfilled.event_index