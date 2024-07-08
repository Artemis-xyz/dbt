{% macro chainlink_fm_rewards_daily(chain) %}
with
    fm_reward_evt_transfer as (
        select
            to_address as admin_address
            , MAX(operator_name) as operator_name
            , MAX(reward_evt_transfer.block_timestamp) as evt_block_time
            , MAX(amount) as token_value
        from {{chain}}_flipside.core.ez_token_transfers reward_evt_transfer
            inner join {{ ref('dim_chainlink_'~chain~'_price_feeds_oracle_addresses') }} price_feeds ON lower(price_feeds.aggregator_address) = lower(reward_evt_transfer.from_address)
            left join {{ ref('dim_chainlink_'~chain~'_ocr_operator_admin_meta') }} fm_operator_admin_meta ON lower(fm_operator_admin_meta.admin_address) = lower(reward_evt_transfer.to_address)
        group by
            tx_hash
            , event_index
            , to_address
    )
    , fm_reward_evt_transfer_daily as (
        SELECT
            evt_block_time::date AS date_start
            , MAX(cast(date_trunc('month', evt_block_time) AS date)) AS date_month
            , fm_reward_evt_transfer.admin_address as admin_address
            , MAX(fm_reward_evt_transfer.operator_name) as operator_name
            , SUM(token_value) as token_amount
        FROM fm_reward_evt_transfer
        LEFT JOIN {{ ref('dim_chainlink_'~chain~'_ocr_operator_admin_meta') }} fm_operator_admin_meta using(admin_address)
        GROUP BY date_start, admin_address
    )
    , admin_address_meta as (
        select distinct admin_address
        FROM fm_reward_evt_transfer_daily
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
                    max(fm_reward_evt_transfer_daily.date_start)
                from fm_reward_evt_transfer_daily
                where fm_reward_evt_transfer_daily.date_start <= link_usd_daily_expanded_by_admin_address.date_start
                    and lower(fm_reward_evt_transfer_daily.admin_address) = lower(link_usd_daily_expanded_by_admin_address.admin_address)
            ) as prev_payment_date
            , (
                select
                    min(fm_reward_evt_transfer_daily.date_start)
                from fm_reward_evt_transfer_daily
                where fm_reward_evt_transfer_daily.date_start > link_usd_daily_expanded_by_admin_address.date_start
                    and lower(fm_reward_evt_transfer_daily.admin_address) = lower(link_usd_daily_expanded_by_admin_address.admin_address)
            ) as next_payment_date
        from link_usd_daily_expanded_by_admin_address
        order by 1, 2
    )
    , fm_reward_daily AS (
        select
            payment_meta.date_start
            , cast(date_trunc('month', payment_meta.date_start) as date) as date_month
            , payment_meta.admin_address
            , ocr_operator_admin_meta.operator_name
            , COALESCE(fm_reward_evt_transfer_daily.token_amount / DATEDIFF(day, prev_payment_date, next_payment_date), 0) as token_amount
            , (COALESCE(fm_reward_evt_transfer_daily.token_amount / DATEDIFF(day, prev_payment_date, next_payment_date), 0) * payment_meta.usd_amount) as usd_amount
        from payment_meta
        left join fm_reward_evt_transfer_daily ON
            payment_meta.next_payment_date = fm_reward_evt_transfer_daily.date_start AND
            lower(payment_meta.admin_address) = lower(fm_reward_evt_transfer_daily.admin_address)
        left join {{ ref('dim_chainlink_'~chain~'_ocr_operator_admin_meta') }} ocr_operator_admin_meta ON lower(ocr_operator_admin_meta.admin_address) = lower(fm_reward_evt_transfer_daily.admin_address)
        order by date_start
    )
select
'{{chain}}' as chain
, date_start
, date_month
, admin_address
, operator_name
, token_amount
, usd_amount
from fm_reward_daily
order by 2, 4

{% endmacro %}