{{
    config(
        materialized="table",
        snowflake_warehouse="TON",
    )
}}

with latest_source_json as (
    select extraction_date, source_url, source_json
    from LANDING_DATABASE.PROD_LANDING.raw_ton_supply
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

, combined_supply as (
        select
            date
            , max_supply
            , total_supply
            , the_open_network_foundation + frozen as foundation_owned
            , issued_supply as issued_supply_native
            , backers + dns_locked + telegram + the_locker as unvested_tokens
            , circulating_supply_first_principles as circulating_supply_native
        from pc_dbt_db.PROD.ton_daily_supply_data

        union

        select
            date
            , total_supply as max_supply
            , total_supply
            , null as foundation_owned
            , null as issued_supply_native
            , null as unvested_tokens
            , circulating_supply as circulating_supply_native
        from filtered_data 
        where date > '2025-06-29'
)

, filled_supply as (
    select
        date(date) as date,
        total_supply,
        circulating_supply_native,
        max_supply,
        last_value(foundation_owned ignore nulls) over (order by date rows between unbounded preceding and current row) as foundation_owned,
        last_value(issued_supply_native ignore nulls) over (order by date rows between unbounded preceding and current row) as issued_supply_native,
        last_value(unvested_tokens ignore nulls) over (order by date rows between unbounded preceding and current row) as unvested_tokens
    from combined_supply
)

, first_principles_calculation as (
    select
        date
        , max_supply as max_supply_native
        , total_supply as total_supply_native
        , foundation_owned
        , total_supply - foundation_owned as issued_supply_native
        , unvested_tokens
        , issued_supply_native - unvested_tokens as circulating_supply_native
    from filled_supply
)

, calculating_net_supply_change as (
    select
        date
        , max_supply_native
        , total_supply_native
        , foundation_owned
        , issued_supply_native
        , unvested_tokens
        , circulating_supply_native - nullif(lag(circulating_supply_native) over (order by date), 0) as net_supply_change_native
        , circulating_supply_native
    from first_principles_calculation
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
        , coalesce(net_supply_change_native, 0) - coalesce(gross_emissions_native, 0) + coalesce(burns_native, 0) as premine_unlocks_native
        , coalesce(gross_emissions_native, 0) as gross_emissions_native
        , coalesce(burns_native, 0) as burns_native
        , net_supply_change_native
        , total_supply_native
        , max_supply_native
        , foundation_owned
        , issued_supply_native
        , unvested_tokens
        , circulating_supply_native
    from first_principles_calculation first_principles
    left join calculating_net_supply_change using (date)
    left join minted_data using (date)
    left join fundamental_data using (date)
    
)

select
    date
    , premine_unlocks_native
    , gross_emissions_native
    , burns_native
    , net_supply_change_native
    , total_supply_native
    , max_supply_native
    , foundation_owned
    , issued_supply_native
    , unvested_tokens
    , circulating_supply_native
from supply_data