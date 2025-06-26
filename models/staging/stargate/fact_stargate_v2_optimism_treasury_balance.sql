{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="MEDIUM",
    )
}}

with
treasury_data as (
    {{ forward_filled_address_balances(
        artemis_application_id="stargate",
        type="treasury",
        chain="optimism"
    )}}
)

, treasury_balances as (
    select
        date
        , case 
            when substr(t1.symbol, 0, 2) = 'S*' then 'stargate'
            else 'wallet'
        end as protocol        
        , treasury_data.contract_address
        , upper(replace(t1.symbol, 'S*', '')) as symbol
        , balance_native
        , balance
    from treasury_data
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'optimism'
)

, date_spine as (
    SELECT
        date
    FROM {{ref('dim_date_spine')}}
    WHERE date >= '2024-03-06' and date <= to_date(sysdate())
)
, raw_velodrom_ownership as (
    select
        block_timestamp::date as date
        , sum(
            case 
                when (
                    lower(decoded_log:from::string) = lower('0x392AC17A9028515a3bFA6CCe51F8b70306C6bd43') or lower(decoded_log:to::string) = lower('0x392AC17A9028515a3bFA6CCe51F8b70306C6bd43')
                )  and event_name = 'Deposit' then decoded_log:amount::float
                when (
                    lower(decoded_log:from::string) = lower('0x392AC17A9028515a3bFA6CCe51F8b70306C6bd43') or lower(decoded_log:to::string) = lower('0x392AC17A9028515a3bFA6CCe51F8b70306C6bd43')
                ) then -decoded_log:amount::float
                else 0
            end
        ) as balance_change
        , sum(
            case 
                when event_name = 'Deposit' then decoded_log:amount::float
                else -decoded_log:amount::float
            end
        ) as supply_change
    from optimism_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x661b82D574a72ee82dE95E5Ed854441d8202cb7F')
        and (event_name = 'Deposit' or event_name = 'Withdraw')
    group by 1
)
, velodrom_ownership_with_every_date as (
    select
        date_spine.date as date
        , coalesce(balance_change, 0) as balance_change
        , coalesce(supply_change, 0) as supply_change
    from date_spine
    left join raw_velodrom_ownership
        on date_spine.date = raw_velodrom_ownership.date
)
, velodrom_ownership_cumulative as (
    select
        date
        , sum(balance_change) over (order by date) as balance_native
        , sum(supply_change) over (order by date) as total_supply
    from velodrom_ownership_with_every_date
)

, dex_pool as (
    {{forward_filled_balance_for_address(
        chain="optimism",
        address="0x1dE84E1324BaF5a9f3A49A48892fE90cE48456f1"
    )}}
)

, dex_balance_raw as (
    select 
        date
        , 'velodrome' as protocol
        , dex_pool.contract_address
        , t1.symbol
        , balance_native
        , balance
    from dex_pool
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(dex_pool.contract_address) and t1.chain = 'optimism'
    where dex_pool.contract_address in (lower('0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85'), lower('0x296F55F8Fb28E498B858d0BcDA06D955B2Cb3f97'))
)

, velodrome_balances as (
    select
        date
        , balance_native / total_supply as balance_percentage
    from velodrom_ownership_cumulative
)

, dex_balance as (
    select
        dex_balance_raw.date
        , protocol
        , contract_address
        , symbol
        , balance_percentage * balance_native as balance_native
        , balance_percentage * balance as balance
    from dex_balance_raw
    left join velodrome_balances
        on dex_balance_raw.date = velodrome_balances.date
)

, yearn_fi_balance as (
     select
        dex_balance_raw.date
        , 'yearn_fi' as protocol
        , contract_address
        , symbol
        , (1-balance_percentage) * balance_native as balance_native
        , (1-balance_percentage) * balance as balance
    from dex_balance_raw
    left join velodrome_balances
        on dex_balance_raw.date = velodrome_balances.date
)

, velodrome_gauge_pool as (
    {{forward_filled_balance_for_address(
        chain="optimism",
        address="0x661b82D574a72ee82dE95E5Ed854441d8202cb7F"
    )}}
)

, velodrome_gauge_balance as (
    select 
        date
        , 'velodrome' as protocol
        , velodrome_gauge_pool.contract_address
        , t1.symbol
        , balance_native
        , balance
    from velodrome_gauge_pool
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(velodrome_gauge_pool.contract_address) and t1.chain = 'optimism'
    where velodrome_gauge_pool.contract_address in (lower('0x9560e827aF36c94D2Ac33a39bCE1Fe78631088Db'))
)

, balances as (
    select * from treasury_balances
    union all
    select * from dex_balance
    union all
    select * from yearn_fi_balance
    union all
    select * from velodrome_gauge_balance
)

select 
    date
    , protocol
    , 'optimism' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from balances