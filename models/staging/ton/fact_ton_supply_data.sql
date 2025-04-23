{{
    config(
        materialized="table",
        snowflake_warehouse="SEI",
    )
}}

with latest_source_json as (
    select extraction_date, source_url, source_json
    from {{ source("PROD_LANDING", "raw_ton_supply") }}
    order by extraction_date desc
    limit 1
)

, raw_json_data as (
    select
        value:date::timestamp as date
        , value:timestamp::number as timestamp
        , value:circulating_supply::float / 1e9 as circulating_supply
        , value:initiated_supply::float / 1e9 as initiated_supply
        , value:total_supply::float / 1e9 as total_supply
        , value:total_accounts::number as total_accounts
    from latest_source_json, lateral flatten(input => parse_json(source_json))
)

, filtered_data as (
    select
        date
        , max_by(circulating_supply, date) as circulating_supply
        , max_by(initiated_supply, date) as initiated_supply
        , max_by(total_supply, date) as total_supply
        , max_by(total_accounts, date) as total_accounts
    from raw_json_data
    group by 1
)

, raw_data as (
    select
        date(date) as date
        , circulating_supply
        , circulating_supply - nullif(lag(circulating_supply) over (order by date), 0) as premine_unlocks_native
        , initiated_supply
        , total_supply
        , total_accounts
    from filtered_data
)

, minted_data as (
    select 
        date(date) as date
        , sum(block_rewards_native) as gross_emissions_native
    from {{ ref("fact_ton_minted") }}
    group by 1
)

, fundamental_data as (
    select 
        date(date) as date
        , sum(fees_native) as fees_native
        , sum(fees_native) / 2 AS burns_native
    from {{ ref("fact_ton_fundamental_metrics") }}
    group by 1
)

, supply_data as (
    select 
        date
        , coalesce(premine_unlocks_native, 0) as premine_unlocks_native
        , coalesce(gross_emissions_native, 0) as gross_emissions_native
        , coalesce(burns_native, 0) as burns_native
        , coalesce(premine_unlocks_native, 0) + coalesce(gross_emissions_native, 0) - coalesce(burns_native, 0) as net_supply_change_native
        , sum(net_supply_change_native) over (order by date) as circulating_supply_native
    from raw_data
    left join minted_data using (date)
    left join fundamental_data using (date)
)

select
    date
    , premine_unlocks_native
    , gross_emissions_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native
from supply_data