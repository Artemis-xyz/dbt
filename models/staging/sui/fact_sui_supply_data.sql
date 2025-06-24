{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
    )
}}

with supply_data as (
    select
        date(date) as date
        , community_reserves
        , stake_subsidies
        , community_access_program
        , series_a
        , series_b
        , early_contributors
        , mysten_labs_treasury
        , uncreated_tokens
        , max_supply AS max_supply_native
        , total_supply AS total_supply_native
        , foundation_owned_supply AS foundation_owned_supply_native
        , unvested_insider_tokens
        , vested_tokens
        , unvested_tokens AS unvested_tokens_native
    from {{ source('MANUAL_STATIC_TABLES', 'sui_daily_supply_data') }}
)

, date_spine as (
    select
        date
    from {{ ref("dim_date_spine") }}
    where date between 
        (select min(date) from supply_data)
        and 
        (select max(date) from supply_data)
)

, joined as (
    select
        ds.date
        , sd.community_reserves
        , sd.stake_subsidies
        , sd.community_access_program
        , sd.series_a
        , sd.series_b
        , sd.early_contributors
        , sd.mysten_labs_treasury
        , sd.uncreated_tokens
        , sd.max_supply_native
        , sd.total_supply_native
        , sd.foundation_owned_supply_native
        , sd.unvested_insider_tokens
        , sd.vested_tokens
        , sd.unvested_tokens_native
    from date_spine ds
    left join supply_data sd on ds.date = sd.date
)

, forward_filled as (
    select
        date
        , last_value(community_reserves ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as community_reserves
        , last_value(stake_subsidies ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as stake_subsidies
        , last_value(community_access_program ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as community_access_program
        , last_value(series_a ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as series_a
        , last_value(series_b ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as series_b
        , last_value(early_contributors ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as early_contributors
        , last_value(mysten_labs_treasury ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as mysten_labs_treasury
        , last_value(uncreated_tokens ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as uncreated_tokens
        , last_value(max_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as max_supply_native
        , last_value(total_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as total_supply_native
        , last_value(foundation_owned_supply_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as foundation_owned_supply_native
        , last_value(unvested_insider_tokens ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as unvested_insider_tokens
        , last_value(vested_tokens ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as vested_tokens
        , last_value(unvested_tokens_native ignore nulls) over (
            order by date
            rows between unbounded preceding and current row
        ) as unvested_tokens_native
    from joined
)

select 
    date
    , community_reserves
    , stake_subsidies
    , community_access_program
    , series_a
    , series_b
    , early_contributors
    , mysten_labs_treasury
    , uncreated_tokens
    , max_supply_native
    , total_supply_native
    , foundation_owned_supply_native
    , unvested_tokens_native
    , unvested_insider_tokens
    , vested_tokens
    , vested_tokens - LAG(vested_tokens) OVER (ORDER BY date) as gross_emissions_native
from forward_filled
