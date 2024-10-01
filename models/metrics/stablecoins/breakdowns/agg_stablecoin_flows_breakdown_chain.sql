{{
    config(
        materialized="table",
        snowflake_warehouse="STABLECOIN_V2_LG_2"
    )
}}

with
    transfers_data as (
        select chain, inflow
        from {{ref('agg_stablecoin_transfers_l30d')}}
    )
    , inflow_data as (
        select 
            chain, 
            sum(inflow) as stablecoin_supply_inflow
        from transfers_data
        where inflow > 0
        group by 1
    )
    , outflow_data as (
        select 
            chain, 
            sum(inflow) as stablecoin_supply_outflow
        from transfers_data
        where inflow < 0
        group by 1
    )
    , net_flow_data as (
        select 
            chain, 
            sum(inflow) as stablecoin_supply_net_flow
        from transfers_data
        group by 1
    )
select 
    chain, 
    stablecoin_supply_inflow, 
    stablecoin_supply_outflow, 
    stablecoin_supply_net_flow
from net_flow_data
full join outflow_data using (chain)
full join inflow_data using (chain)