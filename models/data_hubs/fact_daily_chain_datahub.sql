{{ config(materialized="table") }}
with
    chain_datahub as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("agg_daily_arbitrum_fundamental_usage"),
                    ref("agg_daily_avalanche_fundamental_usage"),
                    ref("agg_daily_base_fundamental_usage"),
                    ref("agg_daily_bsc_fundamental_usage"),
                    ref("agg_daily_ethereum_fundamental_usage"),
                    ref("agg_daily_near_fundamental_usage"),
                    ref("agg_daily_optimism_fundamental_usage"),
                    ref("agg_daily_polygon_fundamental_usage"),
                    ref("agg_daily_solana_fundamental_usage"),
                    ref("agg_daily_ethereum_revenue_gold"),
                    ref("agg_daily_optimism_revenue_gold"),
                    ref("agg_daily_base_revenue_gold"),
                    ref("agg_daily_polygon_revenue_gold"),
                    ref("agg_daily_polygon_zk_revenue_gold"),
                    ref("agg_daily_gnosis_revenue_gold"),
                    ref("fact_arbitrum_contracts_gold"),
                    ref("fact_avalanche_contracts_gold"),
                    ref("fact_bsc_contracts_gold"),
                    ref("fact_ethereum_contracts_gold"),
                    ref("fact_near_revenue_gold"),
                    ref("fact_linea_contracts_gold"),
                    ref("fact_zora_contracts_gold"),
                    ref("fact_scroll_contracts_gold"),
                    ref("fact_polygon_contracts_gold"),
                    ref("fact_optimism_contracts_gold"),
                    ref("fact_multiversx_daa_gold"),
                    ref("fact_multiversx_txns_gold"),
                    ref("fact_bitcoin_daa_gold"),
                    ref("fact_bitcoin_txns_gold"),
                    ref("fact_bitcoin_fees_revenue_gold"),
                    ref("fact_ethereum_verified_contracts_gold"),
                    ref("agg_daily_arbitrum_revenue_gold"),
                    ref("fact_near_contracts_gold"),
                    ref("fact_osmosis_daa_txns_gold"),
                    ref("fact_osmosis_gas_gas_usd_fees_revenue_gold"),
                    ref("fact_starknet_dau_txns_gas_gas_usd_revenue_gold"),
                    ref("fact_zksync_daa_txns_gas_gas_usd_gold"),
                    ref("fact_zksync_revenue_gold"),
                    ref("fact_zora_daa_gold"),
                    ref("fact_zora_gas_gas_usd_revenue_gold"),
                    ref("fact_zora_txns_gold"),
                    ref("fact_linea_daa_gold"),
                    ref("fact_linea_gas_gas_usd_revenue_gold"),
                    ref("fact_linea_txns_gold"),
                    ref("fact_aptos_daa_txns_gas_gas_usd_revenue_gold"),
                    ref("fact_axelar_daa_txns_gold"),
                    ref("fact_base_contracts_gold"),
                    ref("fact_cardano_daa_gold"),
                    ref("fact_cardano_txns_gold"),
                    ref("fact_cardano_fees_and_revenue_gold"),
                    ref("fact_chiliz_trading_volume_gold"),
                    ref("fact_cosmoshub_daa_gold"),
                    ref("fact_cosmoshub_txns_gold"),
                    ref("fact_cosmoshub_fees_and_revenue_gold"),
                    ref("fact_fantom_contracts_gold"),
                    ref("fact_fantom_daa_gold"),
                    ref("fact_fantom_txns_gold"),
                    ref("fact_fuse_daa_txns_gas_gas_usd_gold"),
                    ref("fact_flow_daa_txns_gold"),
                    ref("ez_sui_metrics"),
                    ref("fact_polygon_zk_daa_txns_gas_usd_gold"),
                    ref("ez_tron_metrics"),
                    ref("fact_scroll_daa_gold"),
                    ref("fact_scroll_gas_gas_usd_revenue_gold"),
                    ref("fact_scroll_txns_gold"),
                    ref("fact_solana_contracts_gold"),
                    ref("fact_stacks_daa_txns_gold"),
                    ref("fact_stacks_native_fees_gold"),
                    ref("fact_gnosis_daa_txns_gas_gas_usd_gold"),
                    ref("fact_flow_fees_revs_gold"),
                    ref("fact_mantle_daa_txns_gas_gas_usd_revenue_gold"),
                    ref("ez_sei_metrics"),
                    ref("fact_stride_daa_gas_usd_txns_gold"),
                    ref("fact_zcash_gas_gas_usd_txns_gold"),
                    ref("fact_parallel_finance_daa_gas_gas_usd_txns_gold"),
                    ref("fact_polkadot_daa_txns_gas_gas_usd_revenue_gold"),
                    ref("fact_acala_daa_txns_gas_gas_usd_revenue_gold"),
                    ref("fact_fantom_gas_gas_usd_fees_revenue_gold"),
                ],
            )
        }}
    ),
    chain_data_collapsed as (
        select
            date,
            chain,
            market_pair,
            max(verified_contracts) as verified_contracts,
            max(coalesce(daa, dau)) as daa,
            max(dau_over_100) as dau_over_100,
            max(coalesce(gas, fees_native)) as gas,
            max(coalesce(gas_usd, fees)) as gas_usd,
            max(txns) as txns,
            max(contract_deployers) as contract_deployers,
            max(contracts_deployed) as contracts_deployed,
            max(revenue) as revenue,
            max(revenue_native) as revenue_native,
            max(fees) as fees,
            max(fees_native) as fees_native,
            max(trading_volume) as trading_volume,
            max(returning_users) as returning_users,
            max(new_users) as new_users,
            max(low_sleep_users) as low_sleep_users,
            max(high_sleep_users) as high_sleep_users,
            max(sybil_users) as sybil_users,
            max(non_sybil_users) as non_sybil_users
        from chain_datahub
        group by date, chain, market_pair
    ),
    chain_data_tagged as (
        select chain_data_collapsed.*, coingecko_id, defillama_chain_name
        from chain_data_collapsed
        left join
            {{ ref("dim_chain") }} as chain
            on chain_data_collapsed.chain = chain.artemis_id
    )
select
    coalesce(chain_data_tagged.date, coingecko.date, defillama_chain.date) as date,
    chain_data_tagged.market_pair as market_pair,
    chain_data_tagged.chain as chain,
    chain_data_tagged.verified_contracts as verified_contracts,
    chain_data_tagged.daa as daa,
    chain_data_tagged.dau_over_100 as dau_over_100,
    chain_data_tagged.returning_users as returning_users,
    chain_data_tagged.new_users as new_users,
    chain_data_tagged.low_sleep_users as low_sleep_users,
    chain_data_tagged.high_sleep_users as high_sleep_users,
    chain_data_tagged.sybil_users as sybil_users,
    chain_data_tagged.non_sybil_users as non_sybil_users,
    chain_data_tagged.gas as gas,
    chain_data_tagged.gas_usd as gas_usd,
    chain_data_tagged.txns as txns,
    chain_data_tagged.revenue as revenue,
    chain_data_tagged.revenue_native as revenue_native,
    chain_data_tagged.fees as fees,
    chain_data_tagged.fees_native as fees_native,
    chain_data_tagged.trading_volume as trading_volume,
    chain_data_tagged.contract_deployers as contract_deployers,
    chain_data_tagged.contracts_deployed as contracts_deployed,
    coingecko.coingecko_id as coingecko_id,
    coalesce(
        chain_data_tagged.defillama_chain_name, defillama_chain.defillama_chain_name
    ) as defillama_chain_name,
    defillama_chain.tvl as defillama_tvl,
    defillama_chain.dex_volumes as defillama_dex_volumes,
    coingecko.shifted_token_price_usd as token_price_usd,
    coingecko.shifted_token_market_cap as token_market_cap,
    coingecko.shifted_token_h24_volume_usd as token_h24_volume_usd
from chain_data_tagged as chain_data_tagged
left join
    {{ ref("fact_coingecko_token_date_adjusted_gold") }} as coingecko
    on chain_data_tagged.coingecko_id = coingecko.coingecko_id
    and chain_data_tagged.date = coingecko.date
full join
    {{ ref("agg_defillama_chain_fees_rev_tvl_dex_vol_gold") }} as defillama_chain
    on chain_data_tagged.defillama_chain_name = defillama_chain.defillama_chain_name
    and chain_data_tagged.date = defillama_chain.date
where chain_data_tagged.date < date_trunc('DAY', sysdate())
