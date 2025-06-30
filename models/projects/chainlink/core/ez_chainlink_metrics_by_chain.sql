{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="core",
        alias="ez_metrics_by_chain"
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
            , sum(usd_amount) as ocr_fees
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
            , sum(usd_amount) as fm_fees
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
            , sum(usd_amount) as automation_fees
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
            , sum(usd_amount) as ccip_fees
        from ccip_models
        group by 1, 2
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
            , sum(usd_amount) as vrf_fees
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
            , sum(usd_amount) as direct_fees
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
            , sum(staking_rewards) as token_incentives
        from staking_incentive_models
        group by 1, 2
    )

select
    date
    , chain
    --Old Metrics needed for compatibility
    , coalesce(ocr_fees, 0) + coalesce(fm_fees, 0) as primary_supply_side_revenue
    , coalesce(automation_fees, 0) + coalesce(ccip_fees, 0) + coalesce(vrf_fees, 0) + coalesce(direct_fees, 0) as secondary_supply_side_revenue
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    

    -- Standardized Metrics
    -- Cash Flow Metrics
    , coalesce(automation_fees, 0) as automation_fees
    , coalesce(ccip_fees, 0) as ccip_fees
    , coalesce(vrf_fees, 0) as vrf_fees
    , coalesce(direct_fees, 0) as direct_fees
    , coalesce(ocr_fees, 0) as ocr_fees
    , coalesce(fm_fees, 0) as fm_fees
    , automation_fees + ccip_fees + vrf_fees + direct_fees + fm_fees + ocr_fees as oracle_fees

    , coalesce(automation_fees, 0) + coalesce(ccip_fees, 0) + coalesce(vrf_fees, 0) + coalesce(direct_fees, 0) as fees
    , coalesce(ocr_fees, 0) + coalesce(fm_fees, 0) as service_fee_allocation

    , 0 as revenue
    , coalesce(ocr_fees, 0) + coalesce(fm_fees, 0) as operating_expenses
    , coalesce(token_incentives, 0) as token_incentives
    , revenue - token_incentives - operating_expenses as earnings

from fm_fees_data
left join orc_fees_data using(date, chain)
left join automation_fees_data using(date, chain)
left join ccip_fees_data using(date, chain)
left join vrf_fees_data using(date, chain)
left join staking_incentives_data using(date, chain)
left join direct_fees_data using(date, chain)
where date < to_date(sysdate())