{{ config(materialized="table") }}
with
    app_datahub as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("agg_daily_app_fundamental_usage"),
                    ref("fact_perpetual_protocol_fees_gold"),
                    ref("fact_perpetual_protocol_trading_volume_gold"),
                    ref("fact_perpetual_protocol_unique_traders_gold"),
                    ref("fact_rabbitx_unique_traders_gold"),
                    ref("fact_rabbitx_trading_volume_gold"),
                    ref("fact_hyperliquid_unique_traders_gold"),
                    ref("fact_hyperliquid_trading_volume_gold"),
                    ref("fact_synthetix_trading_volume_gold"),
                    ref("fact_synthetix_unique_traders_gold"),
                    ref("fact_stargate_bridge_volume_gold"),
                    ref("fact_gains_trading_volume_unique_traders_gold"),
                    ref("fact_gmx_trading_volume_gold"),
                    ref("fact_gmx_unique_traders_gold"),
                    ref("fact_gmx_v2_trading_volume_unique_traders_gold"),
                    ref("fact_level_finance_trading_volume_gold"),
                    ref("fact_level_finance_unique_traders_gold"),
                    ref("fact_mux_trading_volume_unique_traders_gold"),
                    ref("fact_drift_trading_volume_gold"),
                    ref("fact_dydx_trading_volume_gold"),
                    ref("fact_dydx_unique_traders_gold"),
                    ref("fact_vertex_unique_traders_gold"),
                    ref("fact_vertex_trading_volume_gold"),
                    ref("fact_venus_lending_bsc_gold"),
                    ref("fact_lido_staked_eth_count_with_USD_and_change_gold"),
                    ref("fact_meth_staked_eth_count_with_USD_and_change_gold"),
                    ref("fact_binance_staked_eth_count_with_usd_and_change"),
                    ref("fact_sweth_staked_eth_count_with_usd_and_change"),
                    ref("fact_coinbase_staked_eth_count_with_usd_and_change"),
                    ref("fact_rocketpool_staked_eth_count_with_USD_and_change_gold"),
                    ref("fact_frax_staked_eth_count_with_USD_and_change_gold"),
                    ref("fact_stakewise_staked_eth_count_with_USD_and_change_gold"),
                    ref("fact_ethx_staked_eth_count_with_usd_and_change"),
                    ref("fact_sushiswap_arbitrum_gold"),
                    ref("fact_sushiswap_avalanche_gold"),
                    ref("fact_sushiswap_bsc_gold"),
                    ref("fact_sushiswap_ethereum_gold"),
                    ref("fact_sushiswap_gnosis_gold"),
                    ref("fact_uniswap_arbitrum_gold"),
                    ref("fact_uniswap_avalanche_gold"),
                    ref("fact_uniswap_base_gold"),
                    ref("fact_uniswap_bsc_gold"),
                    ref("fact_uniswap_ethereum_gold"),
                    ref("fact_uniswap_optimism_gold"),
                    ref("fact_uniswap_polygon_gold"),
                    ref("fact_pancakeswap_arbitrum_gold"),
                    ref("fact_pancakeswap_base_gold"),
                    ref("fact_pancakeswap_bsc_gold"),
                    ref("fact_pancakeswap_ethereum_gold"),
                    ref("fact_dydx_v4_trading_volume_gold"),
                    ref("fact_dydx_v4_txn_and_trading_fees_gold"),
                    ref("fact_dydx_v4_unique_traders_gold"),
                    ref("fact_dydx_v4_fees_gold"),
                    ref("fact_ktx_finance_trading_volume_gold"),
                    ref("fact_ktx_finance_unique_traders_gold"),
                    ref("fact_holdstation_trading_volume_gold"),
                    ref("fact_holdstation_unique_traders_gold"),
                    ref("fact_apex_trading_volume_gold"),
                    ref("fact_radiant_arbitrum_borrows_deposits_gold"),
                    ref("fact_radiant_bsc_borrows_deposits_gold"),
                    ref("fact_radiant_ethereum_borrows_deposits_gold"),
                    ref("fact_spark_ethereum_borrows_deposits_gold"),
                    ref("fact_seamless_protocol_base_borrows_deposits_gold"),
                    ref("fact_uwu_lend_ethereum_borrows_deposits_gold"),
                    ref("fact_aevo_trading_volume_gold"),
                    ref("fact_across_bridge_volume_gold"),
                    ref("fact_across_bridge_daa_gold"),
                    ref("fact_sonne_optimism_borrows_deposits_gold"),
                    ref("fact_sonne_base_borrows_deposits_gold"),
                    ref("fact_moonwell_base_borrows_deposits_gold"),
                    ref("fact_benqi_avalanche_borrows_deposits_gold"),
                    ref("fact_spark_gnosis_borrows_deposits_gold"),
                    ref("fact_quickswap_polygon_gold"),
                    ref("fact_fraxswap_ethereum_gold"),
                    ref("fact_trader_joe_arbitrum_gold"),
                    ref("fact_trader_joe_avalanche_gold"),
                    ref(
                        "fact_curve_tvl_trading_vol_trading_fees_trading_revenue_unique_traders_arbitrum_gold"
                    ),
                    ref(
                        "fact_curve_tvl_trading_vol_trading_fees_trading_revenue_unique_traders_avalanche_gold"
                    ),
                    ref(
                        "fact_curve_tvl_trading_vol_trading_fees_trading_revenue_unique_traders_ethereum_gold"
                    ),
                    ref(
                        "fact_curve_tvl_trading_vol_trading_fees_trading_revenue_unique_traders_optimism_gold"
                    ),
                    ref(
                        "fact_curve_tvl_trading_vol_trading_fees_trading_revenue_unique_traders_polygon_gold"
                    ),
                    ref("fact_wormhole_bridge_volume_gold"),
                    ref("fact_wormhole_bridge_daa_gold"),
                    ref("fact_synapse_bridge_volume_gold"),
                    ref("fact_synapse_bridge_daa_gold"),
                    ref("fact_maverick_daa_txns_gas_gas_usd_zksync_gold"),
                    ref("fact_maverick_daa_txns_gas_gas_usd_ethereum_gold"),
                    ref("fact_maverick_daa_txns_gas_gas_usd_bsc_gold"),
                    ref("fact_maverick_daa_txns_gas_gas_usd_base_gold"),
                    ref("fact_zksync_era_bridge_bridge_volume_gold"),
                    ref("fact_zksync_era_bridge_bridge_daa_gold"),
                    ref("fact_optimism_bridge_bridge_volume_gold"),
                    ref("fact_optimism_bridge_bridge_daa_gold"),
                    ref("fact_base_bridge_bridge_volume_gold"),
                    ref("fact_base_bridge_bridge_daa_gold"),
                    ref("fact_starknet_bridge_bridge_volume_gold"),
                    ref("fact_starknet_bridge_bridge_daa_gold"),
                    ref("fact_polygon_pos_bridge_bridge_volume_gold"),
                    ref("fact_polygon_pos_bridge_bridge_daa_gold"),
                    ref("fact_avalanche_bridge_bridge_volume_gold"),
                    ref("fact_avalanche_bridge_bridge_daa_gold"),
                    ref("fact_arbitrum_one_bridge_bridge_volume_gold"),
                    ref("fact_arbitrum_one_bridge_bridge_daa_gold"),
                    ref("fact_bluefin_trading_volume_gold"),
                    ref("fact_avantis_trading_volume_gold"),
                    ref("fact_avantis_unique_traders_gold"),
                ],
            )
        }}
    ),
    -- Flatten the mega table such that labels that we have only produces one data
    -- point /
    -- day
    app_data_collapsed as (
        select
            date,
            app,
            max(friendly_name) as friendly_name,
            chain,
            market_pair,
            category,
            max(daa) as daa,
            max(returning_users) as returning_users,
            max(new_users) as new_users,
            max(low_sleep_users) as low_sleep_users,
            max(high_sleep_users) as high_sleep_users,
            max(sybil_users) as sybil_users,
            max(non_sybil_users) as non_sybil_users,
            max(gas) as gas,
            max(gas_usd) as gas_usd,
            max(txns) as txns,
            max(trading_volume) as trading_volume,
            max(unique_traders) as unique_traders,
            max(fees) as fees,
            max(bridge_volume) as bridge_volume,
            max(daily_borrows_usd) as daily_borrows_usd,
            max(daily_supply_usd) as daily_supply_usd,
            max(num_staked_eth) as num_staked_eth,
            max(amount_staked_usd) as amount_staked_usd,
            max(num_staked_eth_net_change) as num_staked_eth_net_change,
            max(amount_staked_usd_net_change) as amount_staked_usd_net_change,
            max(tvl) as tvl,
            max(trading_fees) as trading_fees,
            max(txn_fees) as txn_fees,
            max(inflow) as inflow,
            max(outflow) as outflow,
            max(bridge_daa) as bridge_daa

        from app_datahub
        group by date, app, chain, market_pair, category
    ),
    app_data_tagged as (
        select app_data_collapsed.*, coingecko_id, defillama_protocol_id
        from app_data_collapsed
        left join
            {{ ref("dim_apps_gold") }} as app on app_data_collapsed.app = app.namespace
        where app_data_collapsed.app is not null
    )
select
    coalesce(app_data_tagged.date, coingecko.date, defillama_protocol.date) as date,
    app_data_tagged.app as app,
    app_data_tagged.friendly_name as friendly_name,
    app_data_tagged.chain as chain,
    app_data_tagged.category as category,
    app_data_tagged.market_pair as market_pair,
    app_data_tagged.daa as daa,
    app_data_tagged.new_users as new_users,
    app_data_tagged.returning_users as returning_users,
    app_data_tagged.high_sleep_users as high_sleep_users,
    app_data_tagged.low_sleep_users as low_sleep_users,
    app_data_tagged.sybil_users as sybil_users,
    app_data_tagged.non_sybil_users as non_sybil_users,
    app_data_tagged.gas as gas,
    app_data_tagged.gas_usd as gas_usd,
    app_data_tagged.txns as txns,
    app_data_tagged.trading_volume as trading_volume,
    app_data_tagged.unique_traders as unique_traders,
    app_data_tagged.fees as fees,
    app_data_tagged.bridge_volume as bridge_volume,
    app_data_tagged.inflow as inflow,
    app_data_tagged.outflow as outflow,
    app_data_tagged.bridge_daa as bridge_daa,
    app_data_tagged.daily_borrows_usd as daily_borrows_usd,
    app_data_tagged.daily_supply_usd as daily_supply_usd,
    app_data_tagged.num_staked_eth as num_staked_eth,
    app_data_tagged.amount_staked_usd as amount_staked_usd,
    app_data_tagged.num_staked_eth_net_change as num_staked_eth_net_change,
    app_data_tagged.amount_staked_usd_net_change as amount_staked_usd_net_change,
    app_data_tagged.tvl as tvl,
    app_data_tagged.trading_fees as trading_fees,
    app_data_tagged.txn_fees as txn_fees,
    coingecko.coingecko_id as coingecko_id,
    coalesce(
        app_data_tagged.defillama_protocol_id, defillama_protocol.defillama_protocol_id
    ) as defillama_protocol_id,
    defillama_protocol.tvl as defillama_tvl,
    defillama_protocol.fees as defillama_fees,
    defillama_protocol.revenue as defillama_revenue,
    defillama_protocol.dex_volumes as defillama_dex_volumes,
    coingecko.shifted_token_price_usd as token_price_usd,
    coingecko.shifted_token_market_cap as token_market_cap,
    coingecko.shifted_token_h24_volume_usd as token_h24_volume_usd
from app_data_tagged as app_data_tagged
left join
    {{ ref("fact_coingecko_token_date_adjusted_gold") }} as coingecko
    on app_data_tagged.coingecko_id = coingecko.coingecko_id
    and app_data_tagged.date = coingecko.date
    and app_data_tagged.chain is null
full join
    {{ ref("agg_defillama_protocol_fees_rev_tvl_dex_vol_gold") }} as defillama_protocol
    on app_data_tagged.defillama_protocol_id = defillama_protocol.defillama_protocol_id
    and app_data_tagged.date = defillama_protocol.date
    and app_data_tagged.chain is null
where app_data_tagged.date < date_trunc('DAY', sysdate())
