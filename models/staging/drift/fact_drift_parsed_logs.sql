{{ config(materialized="table") }}

SELECT 
    value:block_date::varchar AS block_date,
    value:EstExternalFillCost::float AS est_external_fill_cost,
    value:LiquidationVolume::float AS liquidation_volume,
    value:OrderFillWithOpenbookV1::float AS order_fill_with_openbook_v1,
    value:OrderFillWithPhoenix::float AS order_fill_with_phoenix,
    value:OrderFilledWithAMM::float AS order_filled_with_amm,
    value:OrderFilledWithAMMJit::float AS order_filled_with_amm_jit,
    value:OrderFilledWithAMMJitLPSplit::float AS order_filled_with_amm_jit_lp_split,
    value:OrderFilledWithLPJit::float AS order_filled_with_lp_jit,
    value:OrderFilledWithMatch::float AS order_filled_with_match,
    value:OrderFilledWithMatchJit::float AS order_filled_with_match_jit,
    value:first_block_time::string AS first_block_time,
    value:last_block_time::string AS last_block_time,
    value:market_index::integer AS market_index,
    value:market_type::integer AS market_type,
    value:total_filler_reward::float AS total_filler_reward,
    value:total_liquidatee_fee::float AS total_liquidatee_fee,
    value:total_liquidator_rebate::float AS total_liquidator_rebate,
    value:total_maker_rebate::float AS total_maker_rebate,
    value:total_revenue::float AS total_revenue,
    value:total_taker_fee::float AS total_taker_fee,
    value:total_volume::float AS total_volume,
    value:trade_count::integer AS trade_count
FROM {{ source("PROD_LANDING", "raw_drift_parsed_logs") }},
lateral flatten(input => parse_json(source_json))
qualify row_number() over (partition by block_date, market_index, market_type order by extraction_date desc) = 1
