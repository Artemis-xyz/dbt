{{
    config(
        materialized="table",
        snowflake_warehouse="STABLECOIN_V2_LG_2"
    )
}}

with
    transfers_data as (
        select transfer_volume, from_category, to_category
        from {{ref('agg_stablecoin_transfers_l30d')}}
        where to_category <> from_category
    )
    , inflow_data as (
        select 
            to_category as category, 
            sum(transfer_volume) as stablecoin_supply_inflow
        from transfers_data
        where to_category is not null
        group by 1
    )
    , outflow_data as (
        select 
            from_category as category, 
            -sum(transfer_volume) as stablecoin_supply_outflow
        from transfers_data
        where from_category is not null
        group by 1
    )
    , net_flow_data as (
        select 
            category
            , sum(stablecoin_supply_net_flow) as stablecoin_supply_net_flow
        from (
            select 
                category
                , stablecoin_supply_inflow as stablecoin_supply_net_flow
            from inflow_data
            union all
            select 
                category
                , stablecoin_supply_outflow as stablecoin_supply_net_flow
            from outflow_data
        )
        group by 1
    )
select 
    category, 
    stablecoin_supply_inflow, 
    stablecoin_supply_outflow, 
    stablecoin_supply_net_flow
from net_flow_data
full join outflow_data using (category)
full join inflow_data using (category)