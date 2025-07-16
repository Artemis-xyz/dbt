{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID"
    )
}}

-- Important Note: We only start indexing from '2025-06-16', hence data before '2025-06-16' is backfilled.

with hyperliquid_genesis as (
    select
        date(extraction_date) as date
        , count(*) as user_balance_count
        , sum(try_to_decimal(f.value[1]::string)) as total_balance
    from landing_database.prod_landing.raw_hyperliquid_supply_data,
        lateral flatten(input => source_json:genesis.userBalances) as f
    group by date(extraction_date)
)

, unvested_tokens as (
    select
        date(extraction_date) as date
        , sum(try_to_decimal(f.value[1]::string)) as unvested_tokens
    from landing_database.prod_landing.raw_hyperliquid_supply_data,
        lateral flatten(input => source_json:nonCirculatingUserBalances) as f
    where f.value[0] = '0x43e9abea1910387c4292bca4b94de81462f8a251'
    group by date(extraction_date)
)

, dead_tokens as (
    select
        date(extraction_date) as date
        , sum(try_to_decimal(f.value[1]::string)) as dead_tokens
    from landing_database.prod_landing.raw_hyperliquid_supply_data,
        lateral flatten(input => source_json:nonCirculatingUserBalances) as f
    where f.value[0] in ('0x0000000000000000000000000000000000000000', '0x000000000000000000000000000000000000dead')
    group by date(extraction_date)
)

, hyperliquid_metrics as (
    select
        date(extraction_date) as date
        , max(try_to_decimal(source_json:maxSupply::string)) as max_supply
        , max(try_to_decimal(source_json:totalSupply::string)) as hype_total_supply
        , max(try_to_decimal(source_json:circulatingSupply::string)) as circulating_supply
        , max(try_to_decimal(source_json:futureEmissions::string)) as future_emissions
    from landing_database.prod_landing.raw_hyperliquid_supply_data
    group by date(extraction_date)
)

, foundation_owned as (
    select
        date(extraction_date) as date
        , sum(try_to_decimal(f.value[1]::string)) as foundation_owned
    from landing_database.prod_landing.raw_hyperliquid_supply_data,
        lateral flatten(input => source_json:genesis.userBalances) as f
    where f.value[0] in (
        '0xd57ecca444a9acb7208d286be439de12dd09de5d', 
        '0xa20fcfa0507fe762011962cc581b95bbbc3bbdba', 
        '0xffffffffffffffffffffffffffffffffffffffff'
    )
    group by date(extraction_date)
)

, burn_tokens as (
    select
        date
        , case
            when date = '2025-06-16' then (max_supply - hype_total_supply)
            else (hype_total_supply - lag(hype_total_supply) over (order by date)) * -1
        end as burn_tokens
    from hyperliquid_metrics
)

, cumulative_burn_tokens as (
    select
        burn.date
        , sum(burn.burn_tokens) over (order by burn.date) as cumulative_burn_tokens
        , sum(burn.burn_tokens) over (order by burn.date) + coalesce(dead.dead_tokens, 0) as burn_tokens
    from burn_tokens burn
    left join dead_tokens dead on dead.date = burn.date
)

, hyperliquid_supply_data as (
    select
        metrics.date
        , metrics.max_supply
        , metrics.future_emissions as uncreated_tokens
        , (metrics.max_supply - metrics.future_emissions) as total_supply
        , cumulative_burns.burn_tokens
        , foundation.foundation_owned
        , (metrics.max_supply - metrics.future_emissions) - foundation.foundation_owned - cumulative_burns.burn_tokens as issued_supply
        , unvested.unvested_tokens
        , (metrics.max_supply - metrics.future_emissions) - foundation.foundation_owned - cumulative_burns.burn_tokens - unvested.unvested_tokens as circulating_supply
    from hyperliquid_metrics metrics
    left join cumulative_burn_tokens cumulative_burns on metrics.date = cumulative_burns.date
    left join foundation_owned foundation on metrics.date = foundation.date
    left join unvested_tokens unvested on metrics.date = unvested.date
)

, date_spine as (
    select
            date
        from {{ ref("dim_date_spine") }}
        -- '2024-11-29' is the hyperliquid genesis date
        where date < to_date(sysdate()) and date >= '2024-11-29'
)

, date_spine_with_supply_data as (
    select
        spine.date
        , supply.max_supply
        , supply.uncreated_tokens
        , supply.total_supply
        , supply.burn_tokens
        , supply.foundation_owned
        , supply.issued_supply
        , supply.unvested_tokens
        , supply.circulating_supply
    from date_spine spine
    left join hyperliquid_supply_data supply on spine.date = supply.date
)

, snapshot_jun16 as (
    select *
    from hyperliquid_supply_data
    where date = '2025-06-16'
)

, with_backfill as (
  select
    sj.date,
    case when sj.date < '2025-06-16' then snap.max_supply else sj.max_supply end as max_supply,
    case when sj.date < '2025-06-16' then snap.uncreated_tokens else sj.uncreated_tokens end as uncreated_tokens,
    case when sj.date < '2025-06-16' then snap.total_supply else sj.total_supply end as total_supply,
    case when sj.date < '2025-06-16' then snap.burn_tokens else sj.burn_tokens end as burn_tokens,
    case when sj.date < '2025-06-16' then snap.foundation_owned else sj.foundation_owned end as foundation_owned,
    case when sj.date < '2025-06-16' then snap.issued_supply else sj.issued_supply end as issued_supply,
    case when sj.date < '2025-06-16' then snap.unvested_tokens else sj.unvested_tokens end as unvested_tokens,
    case when sj.date < '2025-06-16' then snap.circulating_supply else sj.circulating_supply end as circulating_supply
  from date_spine_with_supply_data sj
  cross join snapshot_jun16 snap
)

, final_backfill as (
    select
        date
        , last_value(max_supply ignore nulls) over (order by date rows between unbounded preceding and current row) as max_supply
        , last_value(uncreated_tokens ignore nulls) over (order by date rows between unbounded preceding and current row) as uncreated_tokens
        , last_value(total_supply ignore nulls) over (order by date rows between unbounded preceding and current row) as total_supply
        , last_value(burn_tokens ignore nulls) over (order by date rows between unbounded preceding and current row) as burn_tokens
        , last_value(foundation_owned ignore nulls) over (order by date rows between unbounded preceding and current row) as foundation_owned
        , last_value(issued_supply ignore nulls) over (order by date rows between unbounded preceding and current row) as issued_supply
        , last_value(unvested_tokens ignore nulls) over (order by date rows between unbounded preceding and current row) as unvested_tokens
        , last_value(circulating_supply ignore nulls) over (order by date rows between unbounded preceding and current row) as circulating_supply
    from with_backfill
)

select
    date
    , max_supply
    , uncreated_tokens
    , total_supply
    , burn_tokens
    , foundation_owned
    , issued_supply
    , unvested_tokens
    , circulating_supply
from final_backfill
order by date desc