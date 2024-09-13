{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_goldfinch_combined_events'
    )
}}


-- SeniorPool_evt_DepositMade
SELECT block_timestamp, tx_hash, contract_address, 'deposit' AS tx_type, addr, amount
FROM {{ ref('fact_seniorpool_depositmade') }}

UNION ALL

-- Pool_evt_DepositMade
SELECT block_timestamp, tx_hash, contract_address, 'deposit' AS tx_type, addr, amount
FROM {{ ref('fact_pool_depositmade') }}

UNION ALL

-- InvestorRewards_evt_DepositedAndStaked
SELECT block_timestamp, tx_hash, contract_address, 'deposit' AS tx_type, addr, amount
FROM {{ ref('fact_investorrewards_depositedandstaked') }}

UNION ALL

-- SeniorPool_evt_WithdrawalMade
SELECT block_timestamp, tx_hash, contract_address, 'withdraw' AS tx_type, addr, -(user_amount + reserve_amount) AS amount
FROM {{ ref('fact_seniorpool_withdrawalmade') }}

UNION ALL

-- Pool_evt_WithdrawalMade
SELECT block_timestamp, tx_hash, contract_address, 'withdraw' AS tx_type, addr, -(user_amount + reserve_amount) AS amount
FROM {{ ref('fact_pool_withdrawalmade') }}

UNION ALL

-- InvestorRewards_evt_UnstakedAndWithdrewMultiple
SELECT block_timestamp, tx_hash, contract_address, 'withdraw' AS tx_type, addr, -amount / 0.995 AS amount
FROM {{ ref('fact_investorrewards_unstakedandwithdrew') }}

UNION ALL

-- InvestorRewards_evt_UnstakedAndWithdrew
SELECT block_timestamp, tx_hash, contract_address, 'withdraw' AS tx_type, addr, -amount / 0.995 AS amount
FROM {{ ref('fact_investorrewards_unstakedandwithdrewmultiple') }}

UNION ALL

-- SeniorPool_evt_WithdrawalAddedTo
SELECT block_timestamp, tx_hash, contract_address, 'withdraw_request' AS tx_type, addr, -amount as amount
FROM {{ ref('fact_seniorpool_withdrawaladdedto') }}

UNION ALL

-- SeniorPool_evt_WithdrawalRequested
SELECT block_timestamp, tx_hash, contract_address, 'withdraw_request' AS tx_type, addr, -amount AS amount
FROM {{ ref('fact_seniorpool_withdrawalrequested') }}

UNION ALL

-- SeniorPool_evt_WithdrawalCanceled
SELECT block_timestamp, tx_hash, contract_address, 'withdraw_request' AS tx_type, addr, fidu_canceled + reserve_fidu AS amount
FROM {{ ref('fact_seniorpool_withdrawalcanceled') }}

UNION ALL

-- CreditDesk_evt_DrawdownMade
SELECT block_timestamp, tx_hash, contract_address, 'drawdown' AS tx_type, addr, amount
FROM {{ ref('fact_creditdesk_drawdownmade') }}

UNION ALL

-- SeniorPool_evt_InvestmentMadeInSenior
SELECT block_timestamp, tx_hash, contract_address, 'allocation' AS tx_type, addr, amount
FROM {{ ref('fact_seniorpool_investmentmadeinsenior') }}

UNION ALL

-- SeniorPool_evt_InvestmentMadeInJunior
SELECT block_timestamp, tx_hash, contract_address, 'allocation' AS tx_type, addr, amount
FROM {{ ref('fact_seniorpool_investmentmadeinjunior') }}

UNION ALL

-- SeniorPool_evt_InterestCollected
SELECT block_timestamp, tx_hash, contract_address, 'allocation' AS tx_type, addr, -amount
FROM {{ ref('fact_seniorpool_interestcollected') }}

UNION ALL

-- SeniorPool_evt_PrincipalCollected
SELECT block_timestamp, tx_hash, contract_address, 'allocation' AS tx_type, addr, -amount
FROM {{ ref('fact_seniorpool_principalcollected') }}

UNION ALL

-- Pool_evt_InterestCollected (interest_paid)
SELECT block_timestamp, tx_hash, contract_address, 'interest_paid' AS tx_type, addr, pool_amount + reserve_amount AS amount
FROM {{ ref('fact_pool_interestcollected') }}

UNION ALL

-- Pool_evt_InterestCollected
SELECT block_timestamp, tx_hash, contract_address, 'interest_received' AS tx_type, addr, pool_amount AS amount
FROM {{ ref('fact_pool_interestcollected') }}

UNION ALL

-- Pool_evt_PrincipalCollected
SELECT block_timestamp, tx_hash, contract_address, 'principal_paid' AS tx_type, addr, amount
FROM {{ ref('fact_pool_principalcollected') }}

UNION ALL

-- Pool_evt_PrincipalWrittendown
SELECT block_timestamp, tx_hash, contract_address, 'writedown' AS tx_type, addr, -amount
FROM {{ ref('fact_pool_principalwrittendown') }}

UNION ALL

-- SeniorPool_evt_PrincipalWrittenDown
SELECT block_timestamp, tx_hash, contract_address, 'writedown' AS tx_type, addr, -amount
FROM {{ ref('fact_seniorpool_principalwrittendown') }}

UNION ALL

-- Pool_evt_ReserveFundsCollected
SELECT block_timestamp, tx_hash, contract_address, 'revenue' AS tx_type, NULL AS addr, amount
FROM {{ ref('fact_pool_reservefundscollected') }}

UNION ALL

-- SeniorPool_evt_ReserveFundsCollected
SELECT block_timestamp, tx_hash, contract_address, 'withdrawal_revenue' AS tx_type, NULL AS addr, amount
FROM {{ ref('fact_seniorpool_reservefundscollected') }}

UNION ALL

-- MigratedTranchedPool_evt_DepositMade
SELECT block_timestamp, tx_hash, contract_address, 'deposit' AS tx_type, addr, amount
FROM {{ ref('fact_migratedtranchedpool_depositmade') }}

UNION ALL

-- MigratedTranchedPool_evt_WithdrawalMade
SELECT block_timestamp, tx_hash, contract_address, 'withdraw' AS tx_type, addr, -(interest_withdrawn + principal_withdrawn) AS amount
FROM {{ ref('fact_migratedtranchedpool_withdrawalmade') }}

UNION ALL

-- MigratedTranchedPool_evt_DrawdownMade
SELECT block_timestamp, tx_hash, contract_address, 'drawdown' AS tx_type, addr, amount
FROM {{ ref('fact_migratedtranchedpool_drawdownmade') }}

UNION ALL

-- MigratedTranchedPool_evt_PaymentApplied
SELECT block_timestamp, tx_hash, contract_address, 'interest_paid' AS tx_type, addr, interest_amount AS amount
FROM {{ ref('fact_migratedtranchedpool_paymentapplied') }}

UNION ALL

SELECT block_timestamp, tx_hash, contract_address, 'interest_received' AS tx_type, addr, interest_amount - reserve_amount AS amount
FROM {{ ref('fact_migratedtranchedpool_paymentapplied') }}

UNION ALL

SELECT block_timestamp, tx_hash, contract_address, 'principal_paid' AS tx_type, addr, principal_amount AS amount
FROM {{ ref('fact_migratedtranchedpool_paymentapplied') }}

UNION ALL

SELECT block_timestamp, tx_hash, contract_address, 'revenue' AS tx_type, NULL AS addr, amount
FROM {{ ref('fact_migratedtranchedpool_reservefundscollected') }}