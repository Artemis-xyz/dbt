{{
    config(
        materialized="table",
        snowflake_warehouse="ALGORAND",
        database="algorand",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    fundamental_data AS (
        SELECT * EXCLUDE date, TO_TIMESTAMP_NTZ(date) AS date 
        FROM {{ source('PROD_LANDING', 'ez_algorand_metrics') }}
    )
    , date_spine AS (
        SELECT date
        FROM {{ ref('dim_date_spine') }}
        WHERE date >= '2019-06-01' AND date < TO_DATE(SYSDATE())
    )
    , foundation_balances AS (
        SELECT
            date
            , SUM(balance) AS foundation_owned_balance
        FROM {{ ref('fact_algorand_foundation_balances') }}
        GROUP BY 1
    )
    , unvested_supply AS (
        WITH monthly_unlocks AS (
            SELECT
                date,
                ROUND(
                    SUM(end_user_grant)
                    + SUM(foundation)
                    + SUM(node_running_grant)
                    + SUM(participation_rewards)
                    + SUM(public_sale)
                    + SUM(team_and_investors)
                    , 0
                ) AS vested_supply,
                10000000000 
                - ROUND(
                    SUM(end_user_grant)
                    + SUM(foundation)
                    + SUM(node_running_grant)
                    + SUM(participation_rewards)
                    + SUM(public_sale)
                    + SUM(team_and_investors)
                    , 0
                ) AS unvested_supply
            FROM {{ ref('algorand_monthly_unlocks') }}
            GROUP BY date
        )

        , backfilled_supply AS (
            SELECT
            ds.date,
            LAST_VALUE(vested_supply IGNORE NULLS) OVER (
                ORDER BY ds.date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS vested_supply, 
            LAST_VALUE(unvested_supply IGNORE NULLS) OVER (
                ORDER BY ds.date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS unvested_supply, 
            FROM date_spine AS ds
            LEFT JOIN monthly_unlocks
                ON ds.date = monthly_unlocks.date
            ORDER BY ds.date DESC
        )

        SELECT 
            date, 
            unvested_supply, 
            vested_supply - LAG(vested_supply) OVER (ORDER BY date) AS premine_unlocks
        FROM backfilled_supply
        ORDER BY date DESC
    ) 
    , cumulative_burns AS (
        WITH revenue AS (
            SELECT
                date,
                CASE 
                    WHEN date > '2024-12-31' THEN 0.5 * fees_native
                    ELSE fees_native
                END AS revenue_native
            FROM fundamental_data
        )
        SELECT
            date,
            SUM(revenue_native) OVER (ORDER BY date) AS cumulative_burns
        FROM revenue
        ORDER BY date
    )
    , price as (
        {{ get_coingecko_metrics("algorand") }}
    )
SELECT
    DATE(DATE_TRUNC('DAY', fundamental_data.date)) AS date
    
    -- Standardized Metrics

    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume

    -- Usage Data
    , dau AS chain_dau
    , dau
    , txns AS chain_txns
    , txns

    -- Fee Data
    , fees_native * price AS chain_fees
    , fees_native * price AS fees
    , rewards_algo * price AS validator_fee_allocation

    -- Financial Statements
    , CASE 
        WHEN date > '2024-12-31' THEN 0.5 * fees
        ELSE fees
    END AS revenue

    -- Supply Data
    , COALESCE(premine_unlocks, 0) AS premine_unlocks
    , 10000000000 - cumulative_burns - foundation_owned_balance + unvested_supply AS issued_supply_native
    -- The unvested supply exists in the foundation_owned_balance, so it must be added back to get issued supply and does not need to be subtracted from circulating supply
    , 10000000000 - cumulative_burns - foundation_owned_balance AS circulating_supply_native

    -- Bespoke metrics
    , unique_eoas
    , unique_senders
    , unique_receivers
    , new_eoas
    , unique_pairs
    , unique_eoa_pairs
    , unique_tokens
FROM fundamental_data
LEFT JOIN price USING (date)
LEFT JOIN foundation_balances USING (date)
LEFt JOIN unvested_supply USING (date)
LEFT JOIN cumulative_burns USING (date)