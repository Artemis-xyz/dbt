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
    select date, price, 'maple' as source from mpl_price_maple

    union all

    select date, price, 'syrup' as source from mpl_price_syrup
)

, mpl_prices as (
    SELECT
        mp_maple.date
        , mp_maple.price as maple_price
        , mp_syrup.price as syrup_price
    FROM mpl_price_maple mp_maple
    LEFT JOIN mpl_price_syrup mp_syrup ON mp_maple.date = mp_syrup.date
)

, token_incentives_v1 as (
    select
        date(block_timestamp) as date
        , 'MPL' as token
        , decoded_log:reward / 1e18 as incentive_native
        , (decoded_log:reward / 1e18) * 
            case 
                when date(block_timestamp) > '2024-11-11' then syrup_price 
                else maple_price 
            end as incentive_usd
    from {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    left join mpl_prices p on date(block_timestamp) = p.date
    where contract_address in (select rewards_contract_address from rewards_contracts)
    and event_name = 'RewardPaid'
)

, token_incentives_v2 as (
    select 
        date(block_timestamp) as date
        , 'SYRUP' as token
        , sum(amount) as incentive_native
        , sum(amount_usd) as incentive_usd
    from {{ source('ETHEREUM_FLIPSIDE', 'ez_token_transfers') }}
    where lower(from_address) = lower('0x509712F368255E92410893Ba2E488f40f7E986EA') -- v2 (Syrup Drip contract where users claim drip rewards)
        and symbol = 'SYRUP'
    group by date
)

, token_incentives as (
    select date, token, incentive_native, incentive_usd from token_incentives_v1
    union all
    select date, token, incentive_native, incentive_usd from token_incentives_v2
)

, date_spine as (
    select
        date
    from {{ ref('dim_date_spine') }}
    where date between '2021-05-27' and to_date(sysdate())
)

, token_incentives_none_filled as (
    select
        date_spine.date
        , 'SYRUP' as token
        , coalesce(incentive_native, 0) as incentive_native
        , coalesce(incentive_usd, 0) as incentive_usd
    from date_spine
    left join token_incentives on date_spine.date = token_incentives.date
)

SELECT
    date
    , 'SYRUP' as token
    , incentive_native
    , incentive_usd
from token_incentives_none_filled
