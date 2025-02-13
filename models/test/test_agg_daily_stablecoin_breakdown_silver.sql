{{ config(materialized="incremental", unique_key="unique_id", snowflake_warehouse="STABLECOIN_V2_LG") }}

{% set list_stablecoin_address = var('list_stablecoin_address', "") %}

with
    daily_data as (
        select *
        from {{ref("ez_base_stablecoin_metrics_by_address")}} t1
        {% if is_incremental() and list_stablecoin_address == '' %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        {% if list_stablecoin_address != '' %}
            where
            -- Check if at least one contract in list_stablecoin_address exists in the table
            exists (
                select 1 
                from {{ this }} as t1 
                where lower(t1.contract_address) in (
                    {% for address in list_stablecoin_address %}
                        lower('{{ address }}'){% if not loop.last %}, {% endif %}
                    {% endfor %}
                )
            )
            and lower(t1.contract_address) = lower('{{ new_stablecoin_address }}')
        {% endif %}
        union all
        select *
        from {{ref("ez_arbitrum_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_optimism_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_avalanche_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_polygon_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_ethereum_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_solana_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_tron_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_bsc_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_ton_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_celo_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_mantle_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
        {% endif %}
        union all
        select *
        from {{ref("ez_sui_stablecoin_metrics_by_address")}}
        {% if is_incremental() %}
            where date >= (select dateadd('day', -7, max(date)) from {{ this }})
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