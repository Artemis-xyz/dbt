{{ config(materialized="table") }}

WITH dim_minipools_nodes AS (
    SELECT
        tx_hash,
        block_timestamp,
        DECODED_LOG:minipool::string AS minipool_address,
        DECODED_LOG:node::string AS node_address
    FROM
        ethereum_flipside.core.ez_decoded_event_logs
    WHERE 
        event_name = 'MinipoolCreated'
),
prices AS (
    SELECT
        hour,
        price
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE
        is_native = True
),
block_rewards_to_users AS (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(CASE
            WHEN total_amount_transferred < 8 THEN total_amount_transferred
            WHEN total_amount_transferred >= 24 THEN total_amount_transferred - 24
            WHEN total_amount_transferred >= 16 THEN total_amount_transferred - 16
            ELSE total_amount_transferred - 8
        END) AS cl_users_eth
    FROM (
        SELECT
            block_timestamp,
            DECODED_LOG:amount::number / 1e18 AS total_amount_transferred
        FROM
            ethereum_flipside.core.ez_decoded_event_logs
        WHERE
            event_name = 'EtherDeposited'
            AND contract_address = LOWER('0xae78736Cd615f374D3085123A210448E74Fc6393') 
            AND decoded_log:from::string IN (SELECT DISTINCT minipool_address FROM dim_minipools_nodes)
    )
    GROUP BY 1
),
block_rewards_to_nodes AS (
    SELECT
        DATE(block_timestamp) AS date,
        SUM(CASE
            WHEN total_amount_transferred < 8 THEN total_amount_transferred
            WHEN total_amount_transferred >= 24 THEN total_amount_transferred - 24
            WHEN total_amount_transferred >= 16 THEN total_amount_transferred - 16
            ELSE total_amount_transferred - 8
        END) AS cl_suppliers_eth
    FROM (
        SELECT
            block_timestamp,
            DECODED_LOG:amount::number / 1e18 AS total_amount_transferred
        FROM
            ethereum_flipside.core.ez_decoded_event_logs
        WHERE
            event_name = 'EtherWithdrawn'
            AND contract_address IN (SELECT DISTINCT minipool_address FROM dim_minipools_nodes)
            AND decoded_log:to::string IN (SELECT DISTINCT node_address FROM dim_minipools_nodes)
    )
    GROUP BY 1
)
SELECT
    COALESCE(u.date, n.date) AS date,
    COALESCE(u.cl_users_eth, 0) AS cl_users_eth,
    COALESCE(u.cl_users_eth, 0) * p.price AS cl_users_usd,
    COALESCE(n.cl_suppliers_eth, 0) AS cl_nodes_eth,
    COALESCE(n.cl_suppliers_eth, 0) * p.price AS cl_nodes_usd
FROM
    block_rewards_to_users u
FULL OUTER JOIN block_rewards_to_nodes n ON u.date = n.date
LEFT JOIN prices p ON p.hour = DATE_TRUNC('hour', COALESCE(u.date, n.date))