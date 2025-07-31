{{
    config(
        materialized="table",
        snowflake_warehouse="RENDER",
    )
}}

WITH mints_burns AS (
    SELECT
        block_timestamp::date as date,
        sum(mint_amount/1e8) as amount
    FROM 
    {{ source("SOLANA_FLIPSIDE_DEFI", "fact_token_mint_actions") }}
    WHERE mint = 'rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof'
    GROUP by 1
    UNION ALL
    SELECT 
        block_timestamp::date as date,
        sum(burn_amount/1e8) * -1 as amount
    FROM 
        {{ source("SOLANA_FLIPSIDE_DEFI", "fact_token_burn_actions") }}
    WHERE mint = 'rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof'
    GROUP by 1
)
, daily_net_change as (
    SELECT
        date,
        sum(amount) as net_change
    FROM mints_burns
    GROUP BY 1
)
, date_spine as (
    SELECT date FROM dim_date_spine
    WHERE date between (SELECT MIN(date) FROM daily_net_change) and to_date(sysdate())
)
SELECT
    ds.date,
    'solana' as chain,
    net_change,
    SUM(net_change) OVER (ORDER BY ds.date ASC) as supply_native
FROM date_spine ds
LEFT JOIN daily_net_change USING(date)
