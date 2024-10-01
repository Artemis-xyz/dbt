{{
    config(
        materialized="table",
        snowflake_warehouse="STABLECOIN_V2_LG"
    )
}}

with
    transfers_data as (
        select transfer_volume, from_application, to_application
        from {{ref('agg_stablecoin_transfers_l30d')}}
        where to_app <> from_app
    )
    , inflow_data as (
        select 
            to_application as application
            , sum(transfer_volume) as stablecoin_supply_inflow
        from transfers_data
        where to_application is not null
        group by 1
    )
    , outflow_data as (
        select 
            from_application as application, 
            - sum(transfer_volume) as stablecoin_supply_outflow
        from transfers_data
        where from_application is not null
        group by 1
    )
    , net_flow_data as (
        select 
            application
            , sum(stablecoin_supply_net_flow) as stablecoin_supply_net_flow
        from (
            select 
                application
                , stablecoin_supply_inflow as stablecoin_supply_net_flow
            from inflow_data
            union all
            select 
                application
                , stablecoin_supply_outflow as stablecoin_supply_net_flow
            from outflow_data
        )
        group by 1
    )
select 
    application
    , stablecoin_supply_inflow
    , stablecoin_supply_outflow
    , stablecoin_supply_net_flow
from net_flow_data
full join outflow_data using (application)
full join inflow_data using (application)