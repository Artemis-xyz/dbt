-- depends_on {{ ref("ez_base_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="base",
        database="base",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("base") }}),
    defillama_data as ({{ get_defillama_metrics("base") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("base") }}),
    contract_data as ({{ get_contract_metrics("base") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost, revenue_native, revenue
        from {{ ref("agg_daily_base_revenue") }}
    ),  -- supply side revenue and fees
    nft_metrics as ({{ get_nft_metrics("base") }}),
    p2p_metrics as ({{ get_p2p_metrics("base") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("base") }})

select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    wau,
    mau,
    fees_native,  -- total gas fees paid on l2 by users(L2 Fees)
    fees,
    l1_data_cost_native,  -- fees paid to l1 by sequencer (L1 Fees)
    l1_data_cost,
    revenue_native,  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    revenue,
    avg_txn_fee,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    tvl,
    dex_volumes,
    weekly_contracts_deployed,
    weekly_contract_deployers,
    stablecoin_total_supply,
    stablecoin_txns,
    stablecoin_dau,
    stablecoin_transfer_volume,
    deduped_stablecoin_transfer_volume,
    nft_trading_volume,
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_stablecoin_transfer_volume,
    p2p_transfer_volume,
    coalesce(deduped_stablecoin_transfer_volume, 0) - coalesce(p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume,
    coalesce(dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
from fundamental_data
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
where fundamental_data.date < to_date(sysdate())
