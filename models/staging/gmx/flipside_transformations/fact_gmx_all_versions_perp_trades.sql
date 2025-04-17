{{ config(materialized="table", snowflake_warehouse="GMX") }}

with all_perp_trades as (
    select *
    from {{ref('fact_gmx_v1_perp_trades')}}
    union all 
    select *
    from {{ref('fact_gmx_v2_perp_trades')}}
) select * from all_perp_trades