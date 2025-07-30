{{
    config(
        materialized="incremental",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
            , sum(token_amount) as ocr_fees_native
            , sum(usd_amount) as ocr_fees
        from ocr_models
        group by 1
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
            , sum(usd_amount) as fm_fees
        from fm_models
        group by 1
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
            , sum(token_amount) as automation_fees_native
            , sum(usd_amount) as automation_fees
        from automation_models
        group by 1
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
            -- different tokens are paid out in ccip fees
            , sum(usd_amount) as ccip_fees
        from ccip_models
        group by 1
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
            , sum(usd_amount) as vrf_fees
        from vrf_models
        group by 1
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
            , sum(usd_amount) as direct_fees
        from direct_models
        group by 1
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
            , sum(staking_rewards) as token_incentives
        from staking_incentive_models
        group by 1
    )
    , treasury_data as (
        select
            date
            , treasury_usd
            , treasury_link
        from {{ ref("fact_chainlink_treasury_native_usd")}}
    )
    , token_turnover_metrics as (
        select
            date
            , token_turnover_circulating
            , token_turnover_fdv
            , token_volume
        from {{ ref("fact_chainlink_fdv_and_turnover")}}
    )

    ,issued_supply_metrics as (
        select 
            date,
            max_supply_to_date as max_supply_native,
            total_supply as total_supply_native,
            issued_supply as issued_supply_native,
            floating_supply as circulating_supply_native
        from {{ ref('fact_link_issued_supply_and_float') }}
    )

    
    , price_data as ({{ get_coingecko_metrics("chainlink") }})
    , token_holder_data as (
        select
            date
            , tokenholder_count
        from {{ ref("fact_chainlink_tokenholder_count")}}
    )
    , daily_txns_data as (
        select
            date
            , daily_txns
        from {{ ref("fact_chainlink_daily_txns")}}
    ),
    dau_data as (
        select
            date
            , dau
        from {{ ref("fact_chainlink_dau")}}
    ), 
    supply_data as (
        select *
        from {{ ref("fact_chainlink_supply")}}
    )

select
    date
    , 'chainlink' as app
    , 'Oracle' as category
    --Old Metrics needed for compatibility
    , coalesce(automation_fees, 0) + coalesce(ccip_fees, 0) + coalesce(vrf_fees, 0) + coalesce(direct_fees, 0) as fees
    , coalesce(ocr_fees, 0) + coalesce(fm_fees, 0) as primary_supply_side_revenue
    , fees as secondary_supply_side_revenue
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    , daily_txns as txns
    , dau
    , treasury_usd
    , treasury_link
    -- Standardized Metrics
    -- Market Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Usage Metrics
    , dau as oracle_dau
    , daily_txns as oracle_txns
    -- Cash Flow Metrics
    , coalesce(automation_fees, 0) as automation_fees
    , coalesce(ccip_fees, 0) as ccip_fees
    , coalesce(vrf_fees, 0) as vrf_fees
    , coalesce(direct_fees, 0) as direct_fees
    , coalesce(ocr_fees, 0) as ocr_fees
    , coalesce(fm_fees, 0) as fm_fees
    , automation_fees + ccip_fees + vrf_fees + direct_fees + fm_fees + ocr_fees as oracle_fees
    , automation_fees + ccip_fees + vrf_fees + direct_fees + fm_fees + ocr_fees as ecosystem_revenue
    , ecosystem_revenue as service_fee_allocation
    , 0 as revenue
    , token_incentives
    , primary_supply_side_revenue as operating_expenses
    , revenue - token_incentives - operating_expenses as earnings
    -- Treasury Metrics
    , treasury_usd as treasury
    , treasury_link as treasury_native
    -- Supply Metrics
    , premine_unlocks_native
    , issued_supply_metrics.max_supply_native
    , issued_supply_metrics.total_supply_native
    , issued_supply_metrics.issued_supply_native
    , issued_supply_metrics.circulating_supply_native
    -- Other Metrics
    , token_turnover_circulating
    , token_turnover_fdv
    , tokenholder_count
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fm_fees_data
left join orc_fees_data using (date)
left join automation_fees_data using (date)
left join ccip_fees_data using (date)
left join vrf_fees_data using (date)
left join direct_fees_data using (date)
left join staking_incentives_data using (date)
left join treasury_data using (date)
left join token_turnover_metrics using (date)
left join price_data using (date)
left join token_holder_data using (date)
left join daily_txns_data using (date)
left join dau_data using (date)
left join supply_data using (date)
left join issued_supply_metrics using (date)
where true
{{ ez_metrics_incremental('fm_fees_data.date', backfill_date) }}
and date < to_date(sysdate())