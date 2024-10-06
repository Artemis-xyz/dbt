{{
    config(
        materialized="table",
        snowflake_warehouse="STABLECOIN_V2_LG"
    )
}}

with
    transfers_data as (
        select symbol, inflow
        from {{ref('agg_stablecoin_transfers_l30d')}}
    )
    , inflow_data as (
        select 
            symbol, 
            sum(inflow) as stablecoin_inflow
        from transfers_data
        where inflow > 0
        group by 1
    )
    , outflow_data as (
        select 
            symbol, 
            sum(inflow) as stablecoin_outflow
        from transfers_data
        where inflow < 0
        group by 1
    )
    , net_flow_data as (
        select 
            symbol, 
            sum(inflow) as stablecoin_netflow
        from transfers_data
        group by 1
    )
select 
    symbol, 
    stablecoin_inflow, 
    stablecoin_outflow, 
    stablecoin_netflow
from net_flow_data
full join outflow_data using (symbol)
full join inflow_data using (symbol)