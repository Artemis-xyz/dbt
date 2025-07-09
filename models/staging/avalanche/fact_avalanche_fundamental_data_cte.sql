{{ config(materialized="table") }}

with fundamental_data as ({{ get_fundamental_data_for_chain("avalanche", "v2") }})
select * from fundamental_data