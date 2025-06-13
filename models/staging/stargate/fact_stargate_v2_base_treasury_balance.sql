{{
    config(
        materialized="table",
        unique_key="unique_id",
        snowflake_warehouse="MEDIUM",
    )
}}

with
treasury_data as (
    {{ forward_filled_address_balances(
        artemis_application_id="stargate",
        type="treasury",
        chain="base"
    )}}
)

, treasury_balances as (
    select
        date
        , case 
            when substr(t1.symbol, 0, 2) = 'S*' then 'stargate'
            else 'wallet'
        end as protocol        
        , treasury_data.contract_address
        , upper(replace(t1.symbol, 'S*', '')) as symbol
        , balance_native
        , balance
    from treasury_data
    inner join {{ ref("dim_coingecko_token_map") }} t1
        on lower(t1.contract_address) = lower(treasury_data.contract_address) and t1.chain = 'base'
)

, dates as (
    select
        extraction_date,
        flat_json.value:"date"::string as date,
    from
        {{ source("PROD_LANDING", "raw_stargate_aerodrome_base_balance") }},
        lateral flatten(input => parse_json(source_json)) as flat_json
    group by date, extraction_date
)
, max_extraction_per_day as (
    select date, max(extraction_date) as extraction_date
    from dates
    group by date
)

, flattened_json as (
    select
        extraction_date,
        flat_json.value:"date"::string as date,
        flat_json.value:"balance"::float as balance_native
    from
        {{ source("PROD_LANDING", "raw_stargate_aerodrome_base_balance") }},
        lateral flatten(input => parse_json(source_json)) as flat_json
)
, ve_aero_flattened_data as (
    select t1.*
    from flattened_json t1
    left join max_extraction_per_day t2 on t1.date = t2.date
    where t1.extraction_date = t2.extraction_date
)

, ve_aero_prices as ({{get_coingecko_price_with_latest("aerodrome-finance")}})
, ve_aero_balances as (
    select
        ve_aero_flattened_data.date
        , 'aerodrome' as protocol
        , lower('0xeBf418Fe2512e7E6bd9b87a8F0f294aCDC67e6B4') as contract_address
        , 'veAERO' as symbol
        , balance_native
        , balance_native * price as balance
    from ve_aero_flattened_data
    left join ve_aero_prices
        on ve_aero_flattened_data.date = ve_aero_prices.date
    where ve_aero_flattened_data.date < to_date(sysdate())
)

, balances as (
    select * from treasury_balances
    union all
    select * from ve_aero_balances
)

select 
    date
    , protocol
    , 'base' as chain
    , contract_address
    , symbol
    , balance_native
    , balance
from balances