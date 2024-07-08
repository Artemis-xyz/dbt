{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="core",
        alias="ez_metrics_by_token"
    )
}}
--Right now this model only suppoers ethereum
with
    ocr_models as(
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_ocr_reward_daily")
                ]
            )
        }}
    )
    , orc_fees_data as (
        select
            date_start as date
            , chain
            , 'LINK' as token
            , sum(usd_amount) as ocr_fees
        from ocr_models
        group by 1, 2
    )
    , fm_models as(
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_fm_reward_daily")
                ]
            )
        }}
    )
    , fm_fees_data as (
        select
            date_start as date
            , chain
            , 'LINK' as token
            , sum(usd_amount) as fm_fees
        from fm_models
        group by 1, 2
    )
    , automation_models as(
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_automation_reward_daily")
                ]
            )
        }}
    )
    , automation_fees_data as (
        select
            date_start as date
            , chain
            , 'LINK' as token
            , sum(usd_amount) as automation_fees
        from automation_models
        group by 1, 2
    )
    , ccip_models as(
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_ccip_reward_daily")
                ]
            )
        }}
    )
    , ccip_fees_data as (
        select
            date_start as date
            , chain
            -- different tokens are paid out in ccip fees
            , token
            , sum(usd_amount) as ccip_fees
        from ccip_models
        group by 1, 2, 3
    )
    , vrf_models as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_vrf_rewards_daily")
                ]
            )
        }}
    )
    , vrf_fees_data as (
        select
            date
            , blockchain as chain
            , 'LINK' as token
            , sum(usd_amount) as vrf_fees
        from vrf_models
        group by 1, 2
    )
    , staking_incentive_models as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_staking_rewards")
                ]
            )
        }}
    )
    , staking_incentives_data as (
        select
            date
            , chain
            , 'LINK' as token
            , sum(staking_rewards) as token_incentives
        from staking_incentive_models
        group by 1, 2
    )

select
    date
    , chain
    , token
    , automation_fees
    , ccip_fees
    , vrf_fees
    , coalesce(automation_fees, 0) + coalesce(ccip_fees, 0) + coalesce(vrf_fees, 0) as fees
    , ocr_fees
    , fm_fees
    , coalesce(ocr_fees, 0) + coalesce(fm_fees, 0) as primary_supply_side_revenue
    , fees as secondary_supply_side_revenue
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    , 0 as protocol_revenue
    , primary_supply_side_revenue as operating_expenses
    , token_incentives
    , coalesce(operating_expenses, 0) + coalesce(token_incentives, 0) as total_expenses
    , protocol_revenue - total_expenses as earnings
from fm_fees_data
left join orc_fees_data using(date, chain, token)
left join automation_fees_data using(date, chain, token)
full join ccip_fees_data using(date, chain, token)
left join vrf_fees_data using(date, chain, token)
left join staking_incentives_data using(date, chain, token)
where date < to_date(sysdate())