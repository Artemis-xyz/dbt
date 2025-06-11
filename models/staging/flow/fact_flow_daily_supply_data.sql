{{ config(materialized="table") }}

select * from {{source('MANUAL_STATIC_TABLES', 'flow_daily_supply_data')}}
where date > '2021-04-21'