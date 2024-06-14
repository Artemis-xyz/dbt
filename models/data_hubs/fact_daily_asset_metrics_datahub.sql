{{ config(materialized="table") }}
with
    mega_assets_metrics_table as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("agg_daily_category_fundamental_usage"),
                    ref("fact_coingecko_nft_price_mc_vol_gold"),
                    ref("fact_coingecko_nft_supply_and_owners_gold"),
                    ref("fact_gold_price"),
                    ref("fact_nasdaq_price"),
                    ref("fact_spy_price"),
                    ref("fact_daily_app_datahub"),
                    ref("fact_daily_chain_datahub"),
                    ref("fact_daily_coingecko_token_datahub"),
                    ref("fact_twitter_followers"),
                ],
            )
        }}
    )
select
    date,
    mega_assets_metrics_table.app,
    friendly_name,
    mega_assets_metrics_table.chain,
    mega_assets_metrics_table.category,
    max(verified_contracts) as verified_contracts,
    max(gas) gas,
    max(gas_usd) as gas_usd,
    max(txns) as txns,
    max(daa) as daa,
    max(dau_over_100) as dau_over_100,
    max(new_users) as new_users,
    max(returning_users) as returning_users,
    max(low_sleep_users) as low_sleep_users,
    max(high_sleep_users) as high_sleep_users,
    max(sybil_users) as sybil_users,
    max(non_sybil_users) as non_sybil_users,
    max(revenue) as revenue,
    max(revenue_native) as revenue_native,
    max(unique_traders) as unique_traders,
    max(trading_volume) as trading_volume,
    max(bridge_volume) as bridge_volume,
    max(price) as price,
    max(fees) as fees,
    max(fees_native) as fees_native,
    coingecko_nft_id,
    max(nft_floor_price_usd) as nft_floor_price_usd,
    max(nft_floor_price_native) as nft_floor_price_native,
    max(nft_h24_volume_usd) as nft_h24_volume_usd,
    max(nft_h24_volume_native) as nft_h24_volume_native,
    max(nft_market_cap_usd) as nft_market_cap_usd,
    max(nft_market_cap_native) as nft_market_cap_native,
    max(nft_number_of_unique_addresses) as nft_number_of_unique_addresses,
    max(nft_total_supply) as nft_total_supply,
    mega_assets_metrics_table.coingecko_id,
    max(token_price_usd) as token_price_usd,
    max(token_market_cap) as token_market_cap,
    max(token_h24_volume_usd) as token_h24_volume_usd,
    max(contract_deployers) as contract_deployers,
    max(contracts_deployed) as contracts_deployed,
    mega_assets_metrics_table.defillama_chain_name,
    mega_assets_metrics_table.defillama_protocol_id,
    max(defillama_revenue) as defillama_revenue,
    max(defillama_fees) as defillama_fees,
    max(defillama_tvl) as defillama_tvl,
    max(defillama_dex_volumes) as defillama_dex_volumes,
    max(follower_count) as follower_count,
    twitter_handle as twitter_handle,
    top_assets.data_quality_priority
from mega_assets_metrics_table
left join
    {{ ref("dim_data_quality_top_assets") }} as top_assets
    on (mega_assets_metrics_table.app is not distinct from top_assets.app)
    and (mega_assets_metrics_table.chain is not distinct from top_assets.chain)
    and (mega_assets_metrics_table.category is not distinct from top_assets.category)
    and (
        mega_assets_metrics_table.coingecko_id
        is not distinct from top_assets.coingecko_id
    )
    and (
        mega_assets_metrics_table.defillama_protocol_id
        is not distinct from top_assets.defillama_protocol_id
    )
    and (
        mega_assets_metrics_table.defillama_chain_name
        is not distinct from top_assets.defillama_chain_name
    )
where date < date_trunc('DAY', sysdate())
group by
    date,
    mega_assets_metrics_table.app,
    friendly_name,
    mega_assets_metrics_table.chain,
    mega_assets_metrics_table.category,
    coingecko_nft_id,
    mega_assets_metrics_table.coingecko_id,
    mega_assets_metrics_table.defillama_chain_name,
    mega_assets_metrics_table.defillama_protocol_id,
    twitter_handle,
    top_assets.data_quality_priority
