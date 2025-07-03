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
                    , ref("fact_chainlink_arbitrum_ocr_reward_daily")
                    , ref("fact_chainlink_avalanche_ocr_reward_daily")
                    , ref("fact_chainlink_bsc_ocr_reward_daily")
                    , ref("fact_chainlink_gnosis_ocr_reward_daily")
                    , ref("fact_chainlink_optimism_ocr_reward_daily")
                    , ref("fact_chainlink_polygon_ocr_reward_daily")
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
            , sum(token_amount) as ocr_fees_native
        from ocr_models
        group by 1, 2
    )
    , fm_models as(
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_fm_reward_daily")
                    , ref("fact_chainlink_arbitrum_fm_reward_daily")
                    , ref("fact_chainlink_avalanche_fm_reward_daily")
                    , ref("fact_chainlink_bsc_fm_reward_daily")
                    , ref("fact_chainlink_gnosis_fm_reward_daily")
                    , ref("fact_chainlink_optimism_fm_reward_daily")
                    , ref("fact_chainlink_polygon_fm_reward_daily")
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
            , sum(token_amount) as fm_fees_native
        from fm_models
        group by 1, 2
    )
    , automation_models as(
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_automation_reward_daily")
                    , ref("fact_chainlink_avalanche_automation_reward_daily")
                    , ref("fact_chainlink_bsc_automation_reward_daily")
                    , ref("fact_chainlink_polygon_automation_reward_daily")
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
            , sum(token_amount) as automation_fees_native
        from automation_models
        group by 1, 2
    )
    , ccip_models as(
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_ccip_reward_daily")
                    , ref("fact_chainlink_arbitrum_ccip_reward_daily")
                    , ref("fact_chainlink_avalanche_ccip_reward_daily")
                    , ref("fact_chainlink_base_ccip_reward_daily")
                    , ref("fact_chainlink_bsc_ccip_reward_daily")
                    , ref("fact_chainlink_optimism_ccip_reward_daily")
                    , ref("fact_chainlink_polygon_ccip_reward_daily")
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
            , sum(token_amount) as ccip_fees_native
        from ccip_models
        group by 1, 2, 3
    )
    , vrf_models as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_vrf_rewards_daily")
                    , ref("fact_chainlink_arbitrum_vrf_rewards_daily")
                    , ref("fact_chainlink_avalanche_vrf_rewards_daily")
                    , ref("fact_chainlink_bsc_vrf_rewards_daily")
                    , ref("fact_chainlink_optimism_vrf_rewards_daily")
                    , ref("fact_chainlink_polygon_vrf_rewards_daily")

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
            , sum(token_amount) as vrf_fees_native
        from vrf_models
        group by 1, 2
    )
    , direct_models as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_chainlink_ethereum_direct_rewards_daily")
                    , ref("fact_chainlink_arbitrum_direct_rewards_daily")
                    , ref("fact_chainlink_avalanche_direct_rewards_daily")
                    , ref("fact_chainlink_bsc_direct_rewards_daily")
                    , ref("fact_chainlink_gnosis_direct_rewards_daily")
                    , ref("fact_chainlink_optimism_direct_rewards_daily")
                    , ref("fact_chainlink_polygon_direct_rewards_daily")
                ]
            )
        }}
    )
    , direct_fees_data as (
        select
            date
            , chain
            , 'LINK' as token
            , sum(usd_amount) as direct_fees
            , sum(token_amount) as direct_fees_native
        from direct_models
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
            , sum(staking_rewards_native) as token_incentives_native
        from staking_incentive_models
        group by 1, 2
    )

select
    date
    , chain
    , token

    --Old Metrics needed for compatibility
    , coalesce(ocr_fees, 0) + coalesce(fm_fees, 0) as primary_supply_side_revenue
    , coalesce(ocr_fees_native, 0) + coalesce(fm_fees_native, 0) as primary_supply_side_revenue_native
    , coalesce(automation_fees, 0) + coalesce(ccip_fees, 0) + coalesce(vrf_fees, 0) + coalesce(direct_fees, 0) as secondary_supply_side_revenue
    , coalesce(automation_fees_native, 0) + coalesce(ccip_fees_native, 0) + coalesce(vrf_fees_native, 0) + coalesce(direct_fees_native, 0) as secondary_supply_side_revenue_native
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    , primary_supply_side_revenue_native + secondary_supply_side_revenue_native as total_supply_side_revenue_native


    -- Standardized Metrics
    -- Cash Flow Metrics
    , coalesce(automation_fees, 0) as automation_fees
    , coalesce(automation_fees_native, 0) as automation_fees_native
    , coalesce(ccip_fees, 0) as ccip_fees
    , coalesce(ccip_fees_native, 0) as ccip_fees_native
    , coalesce(vrf_fees, 0) as vrf_fees
    , coalesce(vrf_fees_native, 0) as vrf_fees_native
    , coalesce(direct_fees, 0) as direct_fees
    , coalesce(direct_fees_native, 0) as direct_fees_native
    , coalesce(ocr_fees, 0) as ocr_fees
    , coalesce(ocr_fees_native, 0) as ocr_fees_native
    , coalesce(fm_fees, 0) as fm_fees
    , coalesce(fm_fees_native, 0) as fm_fees_native

    , coalesce(automation_fees, 0) + coalesce(ccip_fees, 0) + coalesce(vrf_fees, 0) + coalesce(direct_fees, 0) as fees
    , coalesce(automation_fees_native, 0) + coalesce(ccip_fees_native, 0) + coalesce(vrf_fees_native, 0) + coalesce(direct_fees_native, 0) as fees_native
    , coalesce(automation_fees, 0) + coalesce(ccip_fees, 0) + coalesce(vrf_fees, 0) + coalesce(direct_fees, 0) as service_fee_allocation
    , coalesce(automation_fees_native, 0) + coalesce(ccip_fees_native, 0) + coalesce(vrf_fees_native, 0) + coalesce(direct_fees_native, 0) as service_fee_allocation_native

    , 0 as revenue
    , 0 as revenue_native

    , coalesce(token_incentives, 0) as token_incentives
    , coalesce(token_incentives_native, 0) as token_incentives_native

    , coalesce(ocr_fees, 0) + coalesce(fm_fees, 0) as operating_expenses
    , coalesce(ocr_fees_native, 0) + coalesce(fm_fees_native, 0) as operating_expenses_native
    , coalesce(operating_expenses, 0) + coalesce(token_incentives, 0) as total_expenses
    , coalesce(operating_expenses_native, 0) + coalesce(token_incentives_native, 0) as total_expenses_native
    , revenue - total_expenses as earnings
    , revenue - total_expenses_native as earnings_native
from fm_fees_data
left join orc_fees_data using(date, chain, token)
left join automation_fees_data using(date, chain, token)
full join ccip_fees_data using(date, chain, token)
left join vrf_fees_data using(date, chain, token)
left join staking_incentives_data using(date, chain, token)
left join direct_fees_data using(date, chain, token)
where date < to_date(sysdate())