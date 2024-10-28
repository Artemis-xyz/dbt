{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH cyclical_withdrawals AS (
    SELECT
        w.block_timestamp AS date,
        w.tx_hash,
        w.block,
        p.pool_name,
        p.asset,
        w.account_ AS lender_id,
        w.sharesToRedeem_ / POWER(10, p.precision) AS shares,
        w.assetsToWithdraw_ / POWER(10, p.precision) AS amount,
        'Withdrawal' AS description
    FROM {{ ref('fact_maple_v2_WithdrawalManager_WithdrawalProcessed') }} w
    LEFT JOIN {{ ref('dim_maple_withdrawal_managers') }} wm ON wm.withdrawal_manager = w.contract_address
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.pool_id = wm.pool_id
),

queue_withdrawals AS (
    SELECT
        w.block_timestamp AS date,
        w.tx_hash,
        w.block,
        p.pool_name,
        p.asset,
        w.owner_ AS lender_id,
        w.shares_ / POWER(10, p.precision) AS shares,
        w.assets_ / POWER(10, p.precision) AS amount,
        'Withdrawal' AS description
    FROM {{ ref('fact_maple_v2_QueueWithdrawalManager_RequestProcessed') }} w
    LEFT JOIN {{ ref('dim_maple_withdrawal_managers') }} wm ON wm.withdrawal_manager = w.contract_address
    LEFT JOIN {{ ref('dim_maple_pools') }} p ON p.pool_id = wm.pool_id
),

all_withdrawals AS (
    SELECT * FROM cyclical_withdrawals
    UNION ALL
    SELECT * FROM queue_withdrawals
)

SELECT * FROM all_withdrawals
