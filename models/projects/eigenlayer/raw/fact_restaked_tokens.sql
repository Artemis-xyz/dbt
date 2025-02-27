{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="RAW",
        alias="fact_restaked_tokens",
    )
}}


WITH DepositsIntoStrategy AS (
    SELECT
        DATE(block_timestamp) AS day,
        from_address,
        decoded_input_data:strategy::STRING AS strategy_address,
        decoded_input_data:token::STRING AS token_address,
        SUM(CAST(decoded_input_data:amount AS BIGINT)) AS total_deposited
    FROM {{ source("ETHEREUM_FLIPSIDE", "ez_decoded_traces")}}
    WHERE TO_ADDRESS = LOWER('0x858646372CC42E1A627fcE94aa7A7033e7CF075A')
    AND FUNCTION_NAME = 'depositIntoStrategy'
    --AND decoded_input_data:strategy::STRING = LOWER('0x57ba429517c3473B6d34CA9aCd56c0e735b94c02') -- Strategy filter for testing
    GROUP BY 1, 2, 3, 4
), BalanceEntries AS (
    SELECT 
        block_timestamp,
        DATE(block_timestamp) AS day,
        address AS strategy_address,
        contract_address AS token_address,
        balance_token,
        ROW_NUMBER() over (
            PARTITION BY DATE(block_timestamp), address, contract_address
            ORDER BY block_timestamp DESC
        ) AS latest_balance_rank
    FROM {{ ref('fact_ethereum_address_balances_by_token') }}  
    --WHERE address = LOWER('0x57ba429517c3473B6d34CA9aCd56c0e735b94c02') -- Strategy filter for testing
), LatestDailyBalances AS (
    SELECT
        day,
        strategy_address,
        token_address,
        balance_token
    FROM BalanceEntries
    WHERE latest_balance_rank = 1
), Dates AS (
    SELECT
        date,
    FROM {{ ref('dim_date_spine') }}  --pc_dbt_db.prod.dim_date_spine
    WHERE date BETWEEN '2023-12-01' AND TO_DATE(SYSDATE())
), StrategyTokenCombinations AS (
    SELECT DISTINCT
        strategy_address,
        token_address
    FROM DepositsIntoStrategy
), DateStrategyTokenCombinations AS (
    SELECT
        d.date,
        stc.strategy_address,
        stc.token_address
    FROM Dates d
    CROSS JOIN StrategyTokenCombinations stc
), FinalResult AS (
    SELECT
        dst.date,
        dst.strategy_address,
        dst.token_address,
        ldb.balance_token
    FROM DateStrategyTokenCombinations dst
    LEFT JOIN LatestDailyBalances ldb
        ON dst.date = ldb.day
        AND dst.strategy_address = ldb.strategy_address
        AND dst.token_address = ldb.token_address
), FrontFilledBalances AS (
    SELECT
        date,
        strategy_address,
        token_address,
        COALESCE(
            balance_token,
            LAST_VALUE(balance_token) IGNORE NULLS OVER (
                PARTITION BY strategy_address, token_address 
                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0 -- If no historic record exists, set balance_token to 0
        ) AS balance_token_filled
    FROM FinalResult
)

SELECT 
    date,
    strategy_address,
    token_address,
    balance_token_filled AS balance_token
FROM FrontFilledBalances
ORDER BY date ASC