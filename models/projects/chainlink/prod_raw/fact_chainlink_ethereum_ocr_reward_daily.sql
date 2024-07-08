{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ocr_reward_daily",
    )
}}
with
    admin_address_meta as (
        select distinct admin_address
        from {{ref('fact_chainlink_ethereum_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily
    )
    , link_usd_daily as ({{get_coingecko_price_with_latest("chainlink")}})
    , link_usd_daily_expanded_by_admin_address as (
        select
            date as date_start
            , price as usd_amount
            , admin_address
        from link_usd_daily
        cross join admin_address_meta
        order by
            date_start
            , admin_address
    )
    , payment_meta as (
        select
            date_start
            , link_usd_daily_expanded_by_admin_address.admin_address as admin_address
            , usd_amount
            , (
                select 
                    MAX(ocr_reward_evt_transfer_daily.date_start)
                from
                    {{ref('fact_chainlink_ethereum_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily
                where ocr_reward_evt_transfer_daily.date_start <= link_usd_daily_expanded_by_admin_address.date_start
                    and lower(ocr_reward_evt_transfer_daily.admin_address) = lower(link_usd_daily_expanded_by_admin_address.admin_address)
            ) as prev_payment_date
            , (
                select
                    min(ocr_reward_evt_transfer_daily.date_start)
                from {{ref('fact_chainlink_ethereum_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily
                where ocr_reward_evt_transfer_daily.date_start > link_usd_daily_expanded_by_admin_address.date_start
                    and lower(ocr_reward_evt_transfer_daily.admin_address) = lower(link_usd_daily_expanded_by_admin_address.admin_address)
            ) as next_payment_date
        from link_usd_daily_expanded_by_admin_address
        order by
        1, 2
    )
    , ocr_reward_daily as (
        select
            payment_meta.date_start
            , cast(date_trunc('month', payment_meta.date_start) as date) as date_month
            , payment_meta.admin_address
            , ocr_operator_admin_meta.operator_name
            , COALESCE(ocr_reward_evt_transfer_daily.token_amount / DATEDIFF(day, prev_payment_date, next_payment_date), 0) as token_amount
            , (COALESCE(ocr_reward_evt_transfer_daily.token_amount / DATEDIFF(day, prev_payment_date, next_payment_date), 0) * payment_meta.usd_amount) as usd_amount
        from payment_meta
        left join {{ref('fact_chainlink_ethereum_ocr_reward_evt_transfer_daily')}} ocr_reward_evt_transfer_daily 
            on payment_meta.next_payment_date = ocr_reward_evt_transfer_daily.date_start
            and lower(payment_meta.admin_address) = lower(ocr_reward_evt_transfer_daily.admin_address)
        left join {{ ref('dim_chainlink_ethereum_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON lower(ocr_operator_admin_meta.admin_address) = lower(ocr_reward_evt_transfer_daily.admin_address)
        order by date_start
    )
select
    'ethereum' as chain
    , date_start
    , date_month
    , admin_address
    , operator_name
    , token_amount
    , usd_amount
from ocr_reward_daily
order by 2, 4