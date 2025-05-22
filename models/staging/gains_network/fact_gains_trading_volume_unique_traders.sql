{{ config(materialized="incremental", unique_key="UNIQUE_KEY") }}
with
    arbitrum as (
        select
            block_timestamp::date as date,
            decoded_log:"positionSizeDai"::number / 1e18 as positionsizedai,
            full_decoded_log:data[1]:value[7]::number as leverage,
            positionsizedai * leverage as volume,
            full_decoded_log:data[1]:value[0] as trader
        from arbitrum_flipside.core.ez_decoded_event_logs
        where
            lower(contract_address)
            = lower('0x298a695906e16aeA0a184A2815A76eAd1a0b7522')
            and (event_name = 'MarketExecuted')
            {% if is_incremental() %}
                and block_timestamp >= (select max(date) from {{ this }})
            {% endif %}
        union
        select
            block_timestamp::date as date,
            decoded_log:"positionSizeDai"::number / 1e18 as positionsizedai,
            full_decoded_log:data[0]:value[7]::number as leverage,
            positionsizedai * leverage as volume,
            full_decoded_log:data[0]:value[0] as trader

        from arbitrum_flipside.core.ez_decoded_event_logs
        where
            lower(contract_address)
            = lower('0x298a695906e16aeA0a184A2815A76eAd1a0b7522')
            and (event_name = 'LimitExecuted')
            {% if is_incremental() %}
                and block_timestamp >= (select max(date) from {{ this }})
            {% endif %}
    ),

    polygon as (
        select
            block_timestamp::date as date,
            decoded_log:"positionSizeDai"::number / 1e18 as positionsizedai,
            full_decoded_log:data[1]:value[7]::number as leverage,
            positionsizedai * leverage as volume,
            full_decoded_log:data[1]:value[0] as trader
        from polygon_flipside.core.ez_decoded_event_logs
        where
            lower(contract_address)
            = lower('0x82e59334da8c667797009bbe82473b55c7a6b311')
            and (event_name = 'MarketExecuted')
            {% if is_incremental() %}
                and block_timestamp >= (select max(date) from {{ this }})
            {% endif %}
        union
        select
            block_timestamp::date as date,
            decoded_log:"positionSizeDai"::number / 1e18 as positionsizedai,
            full_decoded_log:data[0]:value[7]::number as leverage,
            positionsizedai * leverage as volume,
            full_decoded_log:data[0]:value[0] as trader
        from polygon_flipside.core.ez_decoded_event_logs
        where
            lower(contract_address)
            = lower('0x82e59334da8c667797009bbe82473b55c7a6b311')
            and (event_name = 'LimitExecuted')
            {% if is_incremental() %}
                and block_timestamp >= (select max(date) from {{ this }})
            {% endif %}
    ),

    raw_gains_trading_volume_unique_traders as (
        select
            date,
            sum(volume) as "VOLUME",
            count(distinct trader) as "UNIQUE_TRADERS",
            'arbitrum' as chain
        from arbitrum
        group by date
        union
        select
            date,
            sum(volume) as "VOLUME",
            count(distinct trader) as "UNIQUE_TRADERS",
            'polygon' as chain
        from polygon
        group by date

    ),

    chain_data as (
        select *, volume as "TRADING_VOLUME", 'gains_network' as app, 'DeFi' as category
        from raw_gains_trading_volume_unique_traders
    ),

    total_data as (
        select
            date,
            sum(trading_volume) as trading_volume,
            sum(unique_traders) as unique_traders
        from chain_data
        group by date
    ),

    results as (
        select chain, date, trading_volume, unique_traders, app, category
        from chain_data
        union all
        select
            null as chain,
            date,
            trading_volume,
            unique_traders,
            'gains_network' as app,
            'DeFi' as category
        from total_data
    )
select *, date || coalesce(chain, 'all') as "UNIQUE_KEY"
from results
where date < date_trunc('DAY', sysdate())
