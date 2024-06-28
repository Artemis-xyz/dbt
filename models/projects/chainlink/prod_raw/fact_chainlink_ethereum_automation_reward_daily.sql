{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_automation_reward_daily",
    )
}}


with
    link_usd_daily as ({{get_coingecko_price_with_latest("chainlink")}})
    , automation_reward_daily as (
        select
            automation_performed_daily.date_start
            , cast(date_trunc('month', automation_performed_daily.date_start) as date) as date_month
            , automation_performed_daily.operator_name
            , automation_performed_daily.keeper_address
            , automation_performed_daily.token_amount as token_amount
            , (automation_performed_daily.token_amount * lud.price) as usd_amount
        from {{ref('fact_chainlink_ethereum_automation_performed_daily')}} automation_performed_daily
        left join link_usd_daily lud on lud.date = automation_performed_daily.date_start
        order by date_start
    )
select
    'ethereum' as chain
    , date_start
    , date_month
    , operator_name
    , keeper_address
    , token_amount
    , usd_amount
from automation_reward_daily
order by 2, 5