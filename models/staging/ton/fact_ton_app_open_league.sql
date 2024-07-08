with latest_source_jsons as (
    select
        extraction_date::date as extraction_date,
        max_by(source_url, extraction_date) as source_url,
        max_by(source_json, extraction_date) as source_json
    from {{ source('PROD_LANDING', 'raw_ton_apps_openleague') }}
    group by 1
),
open_league_data as (
    select
        dateadd(day, -1, extraction_date) as date
        , value
        , REGEXP_SUBSTR(source_url, '/season/S([^/]+)/', 1, 1, 'e')::int AS season
        , value:"icon"::string as icon
        , value:"name"::string as name
        , value:"offchain_avg_dau"::float as offchain_avg_dau
        , value:"offchain_non_premium_users" as offchain_non_premium_users
        , value:"offchain_premium_users" as offchain_premium_users
        , value:"offchain_stickiness"::float as offchain_stickiness
        , value:"offchain_total_unique_users" as offchain_total_unique_users
        , value:"onchain_median_tx" as onchain_median_tx
        , value:"onchain_total_tx" as onchain_total_tx
        , value:"onchain_uaw" as onchain_uaw
        , value:"score"::float as score
        , value:"url"::string as url
    from latest_source_jsons, lateral flatten(input => parse_json(source_json:"items"))
    -- Last Day of Season 4 (plus one)
)
SELECT 
    date
    , value as source_json
    , open_league_data.season
    , icon
    , replace(lower(name), ' ', '_') as name
    , offchain_avg_dau
    , offchain_non_premium_users
    , offchain_premium_users
    , offchain_stickiness
    , offchain_total_unique_users
    , onchain_median_tx
    , onchain_total_tx
    , onchain_uaw
    , score
    , url
    , end_date
FROM open_league_data
left join pc_dbt_db.prod.dim_ton_open_league_dates as ol_dates on open_league_data.season = ol_dates.season
where date <= end_date and date < to_date(sysdate())
