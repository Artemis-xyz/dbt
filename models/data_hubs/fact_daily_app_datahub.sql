{{ config(
    materialized="table",
    snowflake_warehouse="PC_DBT_WH"
) }}
with
    app_datahub as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_synapse_bridge_volume_gold"),
                    ref("fact_synapse_bridge_daa_gold"),
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
            chain,
            category,
            max(bridge_volume) as bridge_volume,
            max(inflow) as inflow,
            max(outflow) as outflow,
            max(bridge_daa) as bridge_daa

        from app_datahub
        group by date, app, chain, category
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
    app_data_tagged.chain as chain,
    app_data_tagged.category as category,
    app_data_tagged.bridge_volume as bridge_volume,
    app_data_tagged.inflow as inflow,
    app_data_tagged.outflow as outflow,
    app_data_tagged.bridge_daa as bridge_daa,
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
