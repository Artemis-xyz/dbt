{{
    config(
        materialized='table',
        snowflake_warehouse='GMX',
        database='GMX',
        schema='raw',
        alias='fact_gmx_all_versions_tvl',
    )
}}

WITH gmx_pool_balances AS (
    SELECT 
        MAX(block_timestamp) as block_timestamp,
        date,
        chain,
        version,
        pool_address,
        token_address,
        MAX(token_balance) as token_balance
    FROM (
        SELECT 
            block_timestamp,
            block_timestamp::date as date,
            chain,
            version,
            pool_address,
            token_address,
            token_balance
        FROM PC_DBT_DB.PROD.FACT_GMX_ALL_VERSIONS_BALANCES
    )
    GROUP BY date, chain, version, pool_address, token_address
), token_info AS (
    SELECT 
        'arbitrum' AS chain,
        hour,
        token_address,
        price,
        symbol,
        decimals
    FROM PC_DBT_DB.PROD.FACT_ARBITRUM_HISTORIC_PRICING

    UNION ALL 

    SELECT 
        'avalanche' AS chain,
        hour,
        token_address,
        price,
        symbol,
        decimals
    FROM AVALANCHE_FLIPSIDE.PRICE.ez_prices_hourly
),

tvl_calculations AS (
    SELECT 
        pb.block_timestamp,
        pb.date,
        EXTRACT(HOUR FROM pb.block_timestamp) as hour,
        pb.chain,
        pb.version,
        pb.pool_address,
        pb.token_address,
        pb.token_balance as token_balance_raw,
        t.decimals,
        pb.token_balance / pow(10,t.decimals) as token_balance,
        t.symbol as token_symbol,
        t.price as token_price,
        (pb.token_balance / POW(10, t.decimals)) * t.price AS token_value_usd
    FROM gmx_pool_balances pb
    LEFT JOIN token_info t 
        ON lower(t.token_address) = lower(pb.token_address) 
        AND t.chain = pb.chain
        and t.hour = pb.date
       -- AND t.hour = TIMESTAMPADD(HOUR, 23, pb.date)
),
latest_tvl_per_token AS (
    SELECT 
        tvl.block_timestamp,
        tvl.date,
        tvl.hour,
        tvl.chain,
        tvl.version,
        tvl.pool_address,
        tvl.token_address,
        token_balance_raw,
        token_balance,
        token_symbol,
        token_price,
        tvl.token_value_usd,
        ROW_NUMBER() OVER (
            PARTITION BY tvl.date, tvl.chain, tvl.pool_address, tvl.token_address
            ORDER BY tvl.block_timestamp DESC
        ) AS rank -- Rank by the latest hour for each date, pool, and token
    FROM tvl_calculations tvl
    -- pb.block_timestamps
),
tvl_aggregated_by_token AS (
    SELECT 
        date,
        hour,
        chain,
        version,
        pool_address,
        token_address,
        token_symbol,
        token_price,
        token_balance_raw,
        token_balance,
        token_value_usd AS tvl_token
    FROM latest_tvl_per_token
    WHERE rank = 1 -- Only keep the latest time entry for each token on each date
),

dates AS (
    SELECT 
        date 
    FROM pc_dbt_db.prod.dim_date_spine
    WHERE date BETWEEN '2020-02-27' AND TO_DATE(SYSDATE())
),

pool_token_combinations AS (
    SELECT DISTINCT
        chain,
        version,
        pool_address,
        token_address
    FROM tvl_aggregated_by_token
),

date_pool_token_combinations AS (
    SELECT 
        d.date,
        ptc.chain,
        ptc.version,
        ptc.pool_address,
        ptc.token_address
    FROM dates d
    CROSS JOIN pool_token_combinations ptc
),

final_result AS (
    SELECT 
        dpt.date,
        dpt.chain,
        dpt.version,
        dpt.pool_address,
        dpt.token_address,
        tat.token_symbol,
        token_balance_raw,
        token_balance,
        token_price,
        tat.tvl_token
    FROM date_pool_token_combinations dpt
    LEFT JOIN tvl_aggregated_by_token tat
        ON dpt.date = tat.date
        AND dpt.chain = tat.chain
        AND dpt.version = tat.version
        AND dpt.pool_address = tat.pool_address
        AND dpt.token_address = tat.token_address
),

backfilled_tvl AS (
    SELECT
        date,
        chain,
        version,
        pool_address,
        token_address,
        token_symbol,
        token_balance_raw,
        token_balance,
        token_price,
        -- Fill in tvl_token values using the most recent non-NULL value
        COALESCE(
            tvl_token,
            LAST_VALUE(tvl_token) IGNORE NULLS OVER (
                PARTITION BY chain, version, pool_address, token_address 
                ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0 -- If no historic record exists, set tvl_token to 0
        ) AS tvl_token_filled
    FROM final_result -- Use the output from the previous query
),
adjusted_tvl AS (
    SELECT
        date,
        chain,
        version,
        pool_address,
        token_address,
        token_symbol,
        token_balance_raw,
        token_balance,
        token_price,
        -- Override tvl_token for the specific token address after 2023-09-01
        CASE
            WHEN token_address = '0x6e36556b3ee5aa28def2a8ec3dae30ec2b208739' AND date > '2023-09-01'
                THEN 0
            ELSE tvl_token_filled
        END AS tvl_token_adjusted
    FROM backfilled_tvl
)

SELECT 
    date,
    chain,
    version,
    pool_address,
    token_address,
    token_symbol as token,
    token_balance,
    token_price,
    tvl_token_adjusted
FROM adjusted_tvl
ORDER BY date, chain, version, pool_address, token_address, tvl_token_adjusted DESC
