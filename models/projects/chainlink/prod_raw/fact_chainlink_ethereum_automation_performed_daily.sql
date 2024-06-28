{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_automation_performed_daily",
    )
}}


select
    'ethereum' as chain
    , evt_block_time::date as date_start
    , max(cast(date_trunc('month', evt_block_time) as date)) as date_month
    , automation_performed.keeper_address as keeper_address
    , max(automation_performed.operator_name) as operator_name
    , sum(token_value) as token_amount
FROM
  {{ref('fact_chainlink_ethereum_automation_performed')}} automation_performed
group by 2, 4
order by 2, 4