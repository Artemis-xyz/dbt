{% macro chainlink_automation_rewards_daily(chain) %}
with
    automation_performed as (
        select
            '{{chain}}' as chain
            , max(operator_name) as operator_name
            , max(coalesce(keeper_address, automation_logs.tx_from)) as keeper_address
            , max(automation_logs.block_timestamp) as evt_block_time
            , max(coalesce(decoded_log:"payment"::number, decoded_log:"totalPayment"::number) / 1e18) as token_value
        from
        {{ ref('fact_chainlink_' ~ chain ~ '_automation_upkeep_performed_logs') }} automation_logs
        left join {{ ref('dim_chainlink_' ~ chain ~ '_automation_meta') }} automation_meta ON automation_meta.keeper_address = automation_logs.tx_from
        group by
            tx_hash
            , event_index
            , tx_from
    )
    , automation_performed_daily as (
        select
            '{{chain}}' as chain
            , evt_block_time::date as date_start
            , max(cast(date_trunc('month', evt_block_time) as date)) as date_month
            , automation_performed.keeper_address as keeper_address
            , max(automation_performed.operator_name) as operator_name
            , sum(token_value) as token_amount
        from automation_performed
        group by 2, 4
    )
    , link_usd_daily as ({{get_coingecko_price_with_latest("chainlink")}})
    , automation_reward_daily as (
        select
            automation_performed_daily.date_start
            , cast(date_trunc('month', automation_performed_daily.date_start) as date) as date_month
            , automation_performed_daily.operator_name
            , automation_performed_daily.keeper_address
            , automation_performed_daily.token_amount as token_amount
            , (automation_performed_daily.token_amount * lud.price) as usd_amount
        from automation_performed_daily
        left join link_usd_daily lud on lud.date = automation_performed_daily.date_start
    )
select
    '{{ chain }}' as chain
    , date_start
    , date_month
    , operator_name
    , keeper_address
    , token_amount
    , usd_amount
from automation_reward_daily
order by 2, 5
{% endmacro %}