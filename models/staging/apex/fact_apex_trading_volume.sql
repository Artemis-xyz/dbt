with
    pro_latest_source_json as (
        select
            max(extraction_date) as extraction_date,
            to_date(value:"time"::string) as date,
            value:"symbol"::string as symbol
        from
            {{ source("PROD_LANDING", "raw_apex_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
        group by 2, 3
    ),
    pro_all_data as (
        select
            to_date(value:"time"::string) as date,
            value:"symbol"::string as symbol,
            value:"open"::float as open,
            value:"close"::float as close,
            value:"volume"::float as volume,
            extraction_date,
        from
            {{ source("PROD_LANDING", "raw_apex_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
    ), 
    pro_latest_extration as (
        select pro_all_data.date, pro_all_data.symbol, open, close, volume
        from pro_all_data
        join
            pro_latest_source_json
            on pro_all_data.extraction_date = pro_latest_source_json.extraction_date
            and pro_all_data.date = pro_latest_source_json.date
            and pro_all_data.symbol = pro_latest_source_json.symbol
    ),
    omni_latest_source_json as (
        select
            max(extraction_date) as extraction_date,
            to_date(value:"time"::string) as date,
            value:"symbol"::string as symbol
        from
            {{ source("PROD_LANDING", "raw_apex_omni_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
        group by 2, 3
    ),
    omni_all_data as (
        select
            to_date(value:"time"::string) as date,
            value:"symbol"::string as symbol,
            value:"open"::float as open,
            value:"close"::float as close,
            value:"volume"::float as volume,
            extraction_date
        from
            {{ source("PROD_LANDING", "raw_apex_omni_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
    ),
    omni_latest_extration as (
        select omni_all_data.date, omni_all_data.symbol, open, close, volume
        from omni_all_data
        join
            omni_latest_source_json
            on omni_all_data.extraction_date = omni_latest_source_json.extraction_date
            and omni_all_data.date = omni_latest_source_json.date
            and omni_all_data.symbol = omni_latest_source_json.symbol
    ),
    volume_by_date as (
        select date, 'PRO' as source, sum(((open + close) / 2) * volume) as trading_volume
        from pro_latest_extration
        group by 1

        UNION ALL

        select date, 'OMNI' as source, sum(((open + close) / 2) * volume) as trading_volume
        from omni_latest_extration
        group by 1
        order by 1 asc
    )
    SELECT 
        date,
        sum(trading_volume) as trading_volume,
        'apex' as app,
        'apex' as chain,
        'DeFi' as category
    FROM volume_by_date
    GROUP BY 1