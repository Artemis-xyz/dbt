{% macro chainlink_ocr_rewards_daily(chain) %}
with
    ocr_reward_evt_transfer as (
        select
            reward_evt_transfer.to_address as admin_address
            , max(operator_name) as operator_name
            , max(reward_evt_transfer.block_timestamp) as evt_block_time
            , max(reward_evt_transfer.amount) as token_value
        from {{chain}}_flipside.core.ez_token_transfers reward_evt_transfer
        right join {{ ref('fact_chainlink_'~chain~'_ocr_reward_transmission_logs') }} ocr_reward_transmission_logs 
            on lower(ocr_reward_transmission_logs.contract_address) = lower(reward_evt_transfer.from_address)
        left join {{ ref('dim_chainlink_'~chain~'_ocr_operator_admin_meta') }} ocr_operator_admin_meta 
            on lower(ocr_operator_admin_meta.admin_address) = lower(reward_evt_transfer.to_address)
        where lower(reward_evt_transfer.from_address) in (select lower(contract_address) from {{ ref('fact_chainlink_'~chain~'_ocr_reward_transmission_logs') }})
        group by
            reward_evt_transfer.tx_hash
            , reward_evt_transfer.event_index
            , reward_evt_transfer.to_address
    )
    , ocr_reward_evt_transfer_daily as (
        select
            evt_block_time::date as date_start
            , max(cast(date_trunc('month', evt_block_time) as date)) as date_month
            , ocr_reward_evt_transfer.admin_address as admin_address
            , max(ocr_reward_evt_transfer.operator_name) as operator_name
            , sum(token_value) as token_amount
        from ocr_reward_evt_transfer
        left join {{ ref('dim_chainlink_'~chain~'_ocr_operator_admin_meta') }} ocr_operator_admin_meta using(admin_address)
        group by date_start, admin_address
    )
    , admin_address_meta as (
        select distinct admin_address
        from ocr_reward_evt_transfer_daily
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
                from ocr_reward_evt_transfer_daily
                where ocr_reward_evt_transfer_daily.date_start <= link_usd_daily_expanded_by_admin_address.date_start
                    and lower(ocr_reward_evt_transfer_daily.admin_address) = lower(link_usd_daily_expanded_by_admin_address.admin_address)
            ) as prev_payment_date
            , (
                select
                    min(ocr_reward_evt_transfer_daily.date_start)
                from ocr_reward_evt_transfer_daily
                where ocr_reward_evt_transfer_daily.date_start > link_usd_daily_expanded_by_admin_address.date_start
                    and lower(ocr_reward_evt_transfer_daily.admin_address) = lower(link_usd_daily_expanded_by_admin_address.admin_address)
            ) as next_payment_date
        from link_usd_daily_expanded_by_admin_address
    )
    , ocr_reward_daily as (
        select
            payment_meta.date_start
            , cast(date_trunc('month', payment_meta.date_start) as date) as date_month
            , payment_meta.admin_address
            , ocr_operator_admin_meta.operator_name
            {% if chain == 'polygon' %}
                , COALESCE((ocr_reward_evt_transfer_daily.token_amount + COALESCE(reconcile_daily.token_amount, 0)) / DATEDIFF(day, prev_payment_date, next_payment_date), 0) as token_amount
                , (COALESCE((ocr_reward_evt_transfer_daily.token_amount + COALESCE(reconcile_daily.token_amount, 0)) / DATEDIFF(day, prev_payment_date, next_payment_date), 0) * payment_meta.usd_amount) as usd_amount
            {% else %}
                , COALESCE(ocr_reward_evt_transfer_daily.token_amount / DATEDIFF(day, prev_payment_date, next_payment_date), 0) as token_amount
                , (COALESCE(ocr_reward_evt_transfer_daily.token_amount / DATEDIFF(day, prev_payment_date, next_payment_date), 0) * payment_meta.usd_amount) as usd_amount
            {% endif %}
        from payment_meta
        left join ocr_reward_evt_transfer_daily 
            on payment_meta.next_payment_date = ocr_reward_evt_transfer_daily.date_start
            and lower(payment_meta.admin_address) = lower(ocr_reward_evt_transfer_daily.admin_address)
        left join {{ ref('dim_chainlink_'~chain~'_ocr_operator_admin_meta') }} ocr_operator_admin_meta 
            on lower(ocr_operator_admin_meta.admin_address) = lower(payment_meta.admin_address)
        {% if chain == 'polygon' %}
            LEFT JOIN {{ ref('fact_chainlink_polygon_ocr_reconcile_daily') }} reconcile_daily
                ON reconcile_daily.date_start = payment_meta.date_start
                AND reconcile_daily.admin_address = payment_meta.admin_address
        {% endif %}
    )
select
    '{{chain}}' as chain
    , date_start
    , date_month
    , admin_address
    , operator_name
    , token_amount
    , usd_amount
from ocr_reward_daily
order by 2, 4 
{% endmacro %}