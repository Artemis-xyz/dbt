{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID"
    )
}}


with hyperliquid_genesis as (
    select
        date(extraction_date) as date,
        count(*) as user_balance_count,
        sum(try_to_decimal(f.value[1]::string)) as total_balance
    from {{ source('PROD_LANDING', 'raw_hyperliquid_supply_data') }},
    lateral flatten(input => source_json:genesis.userBalances) as f
    group by date(extraction_date)
)
, unvested_tokens as (
    select
        date(extraction_date) as date
        , sum(try_to_decimal(f.value[1]::string)) as unvested_tokens
    from {{ source('PROD_LANDING', 'raw_hyperliquid_supply_data') }},
        lateral flatten(input => source_json:nonCirculatingUserBalances) as f
    where f.value[0] = '0x43e9abea1910387c4292bca4b94de81462f8a251'
    group by date(extraction_date)
)
, dead_tokens as (
    select
        date(extraction_date) as date
        , sum(try_to_decimal(f.value[1]::string)) as dead_tokens
    from {{ source('PROD_LANDING', 'raw_hyperliquid_supply_data') }}
        lateral flatten(input => source_json:nonCirculatingUserBalances) as f
    where f.value[0] in ('0x0000000000000000000000000000000000000000', '0x000000000000000000000000000000000000dead')
    group by date(extraction_date)
)
, hyperliquid_metrics as (
    select
        date(extraction_date) as date,
        max(try_to_decimal(source_json:maxSupply::string)) as max_supply,
        max(try_to_decimal(source_json:totalSupply::string)) as hype_total_supply,
        max(try_to_decimal(source_json:circulatingSupply::string)) as circulating_supply,
        max(try_to_decimal(source_json:futureEmissions::string)) as future_emissions
    from {{ source('PROD_LANDING', 'raw_hyperliquid_supply_data') }}
    group by date(extraction_date)
)
, foundation_owned as (
    select
        date(extraction_date) as date
        , sum(try_to_decimal(f.value[1]::string)) as foundation_owned
    from {{ source('PROD_LANDING', 'raw_hyperliquid_supply_data') }} supply,
        lateral flatten(input => source_json:genesis.userBalances) as f
    where f.value[0] in ('0xd57ecca444a9acb7208d286be439de12dd09de5d', '0xa20fcfa0507fe762011962cc581b95bbbc3bbdba', '0xffffffffffffffffffffffffffffffffffffffff')
    group by date(extraction_date)
)
, burn_tokens as (
    select
        date
        , case when 
            date = '2025-06-16' then (max_supply - hype_total_supply)
        else (hype_total_supply - lag(hype_total_supply) over (order by date) ) * -1
    end as burn_tokens
    from hyperliquid_metrics
)
, cumulative_burn_tokens as (
    select
        burn_tokens.date
        , sum(burn_tokens) over (order by burn_tokens.date) as cumulative_burn_tokens
        , sum(burn_tokens) over (order by burn_tokens.date) + coalesce(dead_tokens.dead_tokens, 0) as burn_tokens
    from burn_tokens
    left join dead_tokens on dead_tokens.date = burn_tokens.date
)
select
    hype.date
    , max_supply
    -- futureEmissions increases as undistributed tokens are allocated for future distribution
    , future_emissions as uncreated_tokens
    , (max_supply - uncreated_tokens) as total_supply
    , burn_tokens
    , foundation_owned
    , total_supply - foundation_owned - burn_tokens as issued_supply
    , unvested_tokens
    , issued_supply - unvested_tokens as circulating_supply
from hyperliquid_metrics hype
left join hyperliquid_genesis genesis on hype.date = genesis.date
left join unvested_tokens unvested on hype.date = unvested.date
left join foundation_owned foundation on hype.date = foundation.date
left join cumulative_burn_tokens cumulative_burns on hype.date = cumulative_burns.date
order by hype.date desc