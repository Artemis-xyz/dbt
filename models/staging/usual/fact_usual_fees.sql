{{
    config(
        materialized='table',
        snowflake_warehouse='USUAL',
    )
}}

WITH date_spine AS (
    SELECT date 
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date BETWEEN '2023-05-23' AND TO_DATE(SYSDATE())
),

eod_balances_raw AS (
    SELECT 
        block_timestamp::date AS date,
        MAX_BY(balance_token / 1e6, block_timestamp) AS eod_balance
    FROM pc_dbt_db.prod.fact_ethereum_address_balances_by_token 
    WHERE lower(contract_address) = lower('0x136471a34f6ef19fE571EFFC1CA711fdb8E49f2b')
      AND lower(address) = lower('0xdd82875f0840AAD58a455A70B88eEd9F59ceC7c7')
    GROUP BY 1
),

eod_balances_filled AS (
    SELECT 
        ds.date,
        COALESCE(bl.eod_balance, 
                LAST_VALUE(bl.eod_balance IGNORE NULLS) 
                    OVER (PARTITION BY 1 ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ) AS eod_balance
    FROM date_spine ds
    LEFT JOIN eod_balances_raw bl ON ds.date = bl.date
),

daily_prices_raw AS (
    SELECT 
        DATEADD(seconds, decoded_log:updatedAt, '1970-01-01'::timestamp_ntz)::date AS date,
        AVG(decoded_log:price / 1e8) AS price
    FROM ethereum_flipside.core.ez_decoded_event_logs
    WHERE event_name = 'BalanceReported'
      AND lower(contract_address) = lower('0x4c48bcb2160F8e0aDbf9D4F3B034f1e36d1f8b3e')
    GROUP BY 1
),

daily_prices_filled AS (
    SELECT 
        ds.date,
        COALESCE(dp.price, 
                 LAST_VALUE(dp.price IGNORE NULLS) 
                     OVER (PARTITION BY 1 ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ) AS price
    FROM date_spine ds
    LEFT JOIN daily_prices_raw dp ON ds.date = dp.date
),

daily_prices_final AS (
    SELECT 
        date,
        price,
        LAG(price) OVER (ORDER BY date) AS previous_day_price
    FROM daily_prices_filled
),

treasury_revenue_calculation AS (
    SELECT 
        eb.date,
        eb.eod_balance,
        dp.price AS token_price,
        dp.price - dp.previous_day_price AS token_price_delta,
        eb.eod_balance * (dp.price - dp.previous_day_price) AS daily_treasury_revenue
    FROM eod_balances_filled eb
    LEFT JOIN daily_prices_final dp ON eb.date = dp.date
),

usd0pp_unstaking_fees AS (
    SELECT
        DATE(block_timestamp) AS date,
        (CAST(decoded_log:"usd0ppAmount" AS NUMERIC) - CAST(decoded_log:"usd0Amount" AS NUMERIC)) / 1e18 AS treasury_fee
    FROM ethereum_flipside.core.ez_decoded_event_logs
    WHERE event_name = 'Usd0ppUnlockedFloorPrice'
),

agg_usd0pp_unstaking_fees AS (
    SELECT 
        date,
        SUM(treasury_fee) AS treasury_fee
    FROM usd0pp_unstaking_fees
    GROUP BY date
),

usualx_deposits_raw AS (
    SELECT
        DATE(block_timestamp) AS date,
        tx_hash, 
        decoded_log:"assets"/1e18 AS assets,
        decoded_log:"shares"/1e18 AS shares
    FROM ethereum_flipside.core.ez_decoded_event_logs
    WHERE event_name = 'Deposit'
      AND LOWER(contract_address) = LOWER('0x06B964d96f5dCF7Eae9d7C559B09EDCe244d4B8E')
),

usualx_withdraws_raw AS (
    SELECT
        DATE(block_timestamp) AS date,
        tx_hash,
        decoded_log:"assets"/1e18 AS assets,
        decoded_log:"shares"/1e18 AS shares
    FROM ethereum_flipside.core.ez_decoded_event_logs
    WHERE event_name = 'Withdraw'
      AND LOWER(contract_address) = LOWER('0x06B964d96f5dCF7Eae9d7C559B09EDCe244d4B8E')
),

usualx_withdraw_deposit_combined AS (
    SELECT 
        COALESCE(d.date, w.date) AS date,
        COALESCE(d.shares, 0) AS deposit_shares,
        COALESCE(d.assets, 0) AS deposit_assets,
        -COALESCE(w.shares, 0) AS withdraw_shares,
        -COALESCE(w.assets, 0) AS withdraw_assets,
        0.1 * COALESCE(w.assets, 0) AS usualx_unstake_fees
    FROM usualx_deposits_raw d
    FULL OUTER JOIN usualx_withdraws_raw w ON w.tx_hash = d.tx_hash
),

usualx_aggregated_data_deposits_withdraw AS (
    SELECT
        date,
        SUM(usualx_unstake_fees) AS usualx_unstake_fees_daily
    FROM usualx_withdraw_deposit_combined
    GROUP BY date
),

final_agg_data AS (
    SELECT
        tre.date,
        COALESCE(tre.daily_treasury_revenue, 0) AS daily_treasury_revenue,
        COALESCE(usualx.usualx_unstake_fees_daily, 0) AS usualx_unstake_fees_daily,
        COALESCE(usd0pp.treasury_fee, 0) AS treasury_fee
    FROM treasury_revenue_calculation tre
    LEFT JOIN agg_usd0pp_unstaking_fees usd0pp ON tre.date = usd0pp.date
    LEFT JOIN usualx_aggregated_data_deposits_withdraw usualx ON tre.date = usualx.date
)

SELECT
    fa.date,
    fa.daily_treasury_revenue,
    SUM(fa.daily_treasury_revenue) OVER (ORDER BY fa.date) AS cumulative_treasury_revenue,
    fa.usualx_unstake_fees_daily,
    fa.treasury_fee,
    fa.usualx_unstake_fees_daily + fa.treasury_fee AS fees,
    SUM(fa.usualx_unstake_fees_daily + fa.treasury_fee) OVER (ORDER BY fa.date) AS cumulative_fees
FROM final_agg_data fa
ORDER BY fa.date DESC