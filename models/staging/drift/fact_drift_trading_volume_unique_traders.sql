with
    v2_api_data as (
        select *
        from {{ source("PROD_LANDING", "raw_drift_trading_volume_unique_traders") }}
        where source_url like '%tradeRecords%'
    ),
    data as (
        select source_url, max_by(source_json, extraction_date) as source_json
        from v2_api_data
        group by source_url
    ),
    drift_data as (
        select
            flat_json.value:"market_pair"::string as market_pair,
            date(flat_json.value:"date"::int) as date,
            flat_json.value:"maker"::string as maker,
            flat_json.value:"taker"::string as taker,
            flat_json.value:"quoteAssetAmountFilled" as trading_volume
        from data, lateral flatten(input => parse_json(source_json)) as flat_json
    ),
    makers as (
        select date, array_agg(distinct maker) as makers
        from drift_data
        where maker is not null and maker <> 'undefined'
        group by date
    ),
    takers as (
        select date, array_agg(distinct taker) as takers
        from drift_data
        where taker is not null and taker <> 'undefined'
        group by date
    ),
    drift_unique_traders as (
        select date, array_size(array_intersection(makers, takers)) as unique_traders
        from takers
        inner join makers using (date)
        order by date desc
    ),
    drift_trading_by_pair as (
        select market_pair, date, sum(trading_volume) as trading_volume
        from drift_data
        group by market_pair, date
        order by date desc
    ),
    drift_trading_volume as (
        select date, sum(trading_volume) as trading_volume
        from drift_trading_by_pair
        group by date
    ),
    combined_data as (
        select
            drift_unique_traders.date,
            trading_volume,
            unique_traders,
            null as market_pair,
            'solana' as chain,
            'drift' as app,
            'DeFi' as category
        from drift_trading_volume
        inner join
            drift_unique_traders
            on drift_trading_volume.date = drift_unique_traders.date
        union
        select
            date,
            trading_volume,
            null as unique_traders,
            market_pair,
            'solana' as chain,
            'drift' as app,
            'DeFi' as category
        from drift_trading_by_pair
    )
select date, trading_volume, unique_traders, market_pair, chain, app, category
from combined_data
order by date desc
