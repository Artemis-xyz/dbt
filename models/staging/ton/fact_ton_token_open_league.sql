with latest_source_jsons as (
    select
        extraction_date::date as extraction_date,
        max_by(source_url, extraction_date) as source_url,
        max_by(source_json, extraction_date) as source_json
    from {{source('PROD_LANDING', 'raw_ton_tokens_openleague')}}
    group by 1
),
open_league_data as (
    select
        dateadd(day, -1, extraction_date) as date
        , value
        , REGEXP_SUBSTR(source_url, '/season/S([^/]+)/', 1, 1, 'e')::int AS season
        , value:"has_boost" as has_boost
        , value:"boost_link"::string as boost_link
        , value:"icon"::string as icon
        , value:"is_meme" as is_meme
        , value:"name"::string as name
        , value:"new_users_min_amount" as new_users_min_amount
        , value:"price_change_normed"::float as price_change_normed
        , value:"price_change_simple"::float as price_change_simple
        , value:"score"::float as score
        , value:"token_address"::string as token_address
        , value:"token_last_tvl" as token_last_tvl
        , value:"token_price_after"::float as token_price_after
        , value:"token_price_before"::float as token_price_before
        , value:"token_start_tvl" as token_start_tvl
        , value:"token_tvl_change"::string as token_tvl_change
        , value:"url"::string as url
    from latest_source_jsons, lateral flatten(input => parse_json(source_json:"items"))
    -- Last Day of Season 4 (plus one)
)
SELECT 
    date
    , value as source_json
    , open_league_data.season
    , has_boost
    , boost_link
    , icon
    , is_meme
    , name
    , new_users_min_amount
    , price_change_normed
    , price_change_simple
    , score
    , token_address
    , token_last_tvl
    , token_price_after
    , token_price_before
    , token_start_tvl
    , token_tvl_change
    , url
FROM open_league_data
left join pc_dbt_db.prod.dim_ton_open_league_dates as ol_dates on open_league_data.season = ol_dates.season
where date <= end_date and date < to_date(sysdate())
