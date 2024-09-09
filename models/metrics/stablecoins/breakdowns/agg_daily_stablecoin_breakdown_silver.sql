{{ config(materialized="incremental", unique_key="unique_id", snowflake_warehouse="STABLECOIN_V2_LG") }}
with
    daily_data as (
        select *
        from {{ref("ez_base_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_arbitrum_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_optimism_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_avalanche_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_polygon_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_ethereum_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_solana_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_tron_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_bsc_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_ton_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_celo_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
select 
    date
    , from_address
    , contract_name
    , contract
    , application
    , icon
    , app
    , category
    , is_wallet

    , contract_address
    , symbol

    , stablecoin_transfer_volume
    , stablecoin_daily_txns
    , artemis_stablecoin_transfer_volume
    , artemis_stablecoin_daily_txns
    , p2p_stablecoin_transfer_volume
    , p2p_stablecoin_daily_txns
    , stablecoin_supply
    , chain
    , unique_id
from daily_data
where date < to_date(sysdate())