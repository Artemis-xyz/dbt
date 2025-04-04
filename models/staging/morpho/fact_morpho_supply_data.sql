{{ config(materialized="table", snowflake_warehouse="MEDIUM") }}


with morpho_dao_reserve as (
    select
        date
        , user_address
        , contract_address
        , balance
        , dao_reserve_change
    from {{ ref("fact_morpho_dao_reserve") }}
)

, morpho_seed_funding as (
    select
        date
        , user_address
        , contract_address
        , balance
        , seed_funding_change
    from {{ ref("fact_morpho_seed_funding") }}
)

, morpho_wrapper as (
    select
        date
        , user_address
        , contract_address
        , balance
        , wrapper_change
    from {{ ref("fact_morpho_wrapper") }}
)

, morpho_daily_net_change as (
    select
        sf.date
        , sf.balance as seed_balance
        , wm.balance as wrapper_balance
        , drc.balance as dao_balance
        , coalesce(wm.wrapper_change, 0) + 
          coalesce(seed_funding_change, 0) + 
          coalesce(dao_reserve_change, 0) as net_supply_change_native
    from morpho_seed_funding sf
    left join morpho_wrapper wm on sf.date = wm.date
    left join morpho_dao_reserve drc on sf.date = drc.date
)

, daily_morpho_supply as (
    select
        date
        , seed_balance
        , wrapper_balance
        , dao_balance
        , seed_balance + wrapper_balance + dao_balance as premine_unlocks_native
        , net_supply_change_native
        , 1000000000 - premine_unlocks_native as circulating_supply_native
        
    from morpho_daily_net_change
)

select
    date
    -- emissions_native,
    -- burns_native,
    , premine_unlocks_native
    , net_supply_change_native
    , circulating_supply_native
from daily_morpho_supply
order by date desc