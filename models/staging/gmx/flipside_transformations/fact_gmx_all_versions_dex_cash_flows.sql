{{ config(materialized="table", snowflake_warehouse="GMX") }}

with dex_swap_fee_allocations as (
    select
        date,
        chain,
        version,
        count(distinct tx_hash) as spot_txns,
        count(distinct trader) as spot_traders,
        sum(coalesce(volume, 0)) as spot_volume,
        sum(coalesce(fees, 0)) as spot_fees,
        CASE
            WHEN version = 'v1' THEN 0.7 * spot_fees
            WHEN version = 'v2' THEN 0.63 * spot_fees
        END as spot_lp_fee_allocation,
        CASE
            WHEN version = 'v1' THEN 0.3 * spot_fees
            WHEN version = 'v2' THEN 0.27 * spot_fees
        END as spot_stakers_fee_allocation,
        CASE
            WHEN version = 'v1' THEN 0 * spot_fees
            WHEN version = 'v2' THEN 0.012 * spot_fees
        END as spot_oracle_fee_allocation,
        CASE
            WHEN version = 'v1' THEN 0 * spot_fees
            WHEN version = 'v2' THEN 0.088 * spot_fees
        END as spot_treasury_fee_allocation
    from {{ref('fact_gmx_all_versions_dex_swaps')}}
    group by 1,2,3
) select * from dex_swap_fee_allocations