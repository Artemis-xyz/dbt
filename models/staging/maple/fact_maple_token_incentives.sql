{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE'
    )
}}

with rewards_contracts as (
    SELECT
        decoded_log:mplRewards as rewards_contract_address
    FROM
        {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    where
        event_name = 'MplRewardsCreated'
)
, mpl_price_maple as (
    {{ get_coingecko_metrics("maple") }}
)

, mpl_price_syrup as (
    {{ get_coingecko_metrics("syrup") }}
)

, combined_prices as (
    SELECT date, price, 'maple' as source FROM mpl_price_maple
    UNION ALL
    SELECT date, price, 'syrup' as source FROM mpl_price_syrup
)

, mpl_prices as (
    SELECT
        mp_maple.date,
        -- Join both price tables
        mp_maple.price as maple_price,
        mp_syrup.price as syrup_price
    FROM mpl_price_maple mp_maple
    LEFT JOIN mpl_price_syrup mp_syrup ON mp_maple.date = mp_syrup.date
)
SELECT
    date(block_timestamp) as date,
    'SYRUP' as token,
    decoded_log:reward / 1e18 as incentive_native,
    (decoded_log:reward / 1e18) * 
        case when 
            date(block_timestamp) > '2024-11-11' then syrup_price
            else maple_price
        end as incentive_usd
FROM
    {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }} l
LEFT JOIN mpl_prices p on date(block_timestamp) = p.date
WHERE contract_address in (SELECT rewards_contract_address FROM rewards_contracts)
AND event_name = 'RewardPaid'