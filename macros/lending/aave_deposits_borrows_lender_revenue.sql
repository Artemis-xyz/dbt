{% macro aave_deposits_borrows_lender_revenue(chain, protocol, contract_address, raw_table, healed_raw_table) %}
with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        {% if healed_raw_table is defined %}
            select *
            from landing_database.prod_landing.{{healed_raw_table}}
            union all
        {% endif %}
        select *
        from {{ source("PROD_LANDING", raw_table) }}
    ),
    dates as (
        select
            extraction_date,
            to_timestamp(trunc(flat_json.value:"day"::timestamp, 'day')) as date
        from unioned t1, lateral flatten(input => parse_json(source_json)) as flat_json
        group by date, extraction_date
    ),
    max_extraction_per_day as (
        select date, max(extraction_date) as extraction_date
        from dates
        group by date
        order by date
    ),
    flattened_json as (
        select
            extraction_date,
            to_timestamp(trunc(flat_json.value:"day"::timestamp, 'day')) as date,
            flat_json.value:"underlying_token"::string as underlying_token,
            flat_json.value:"underlying_token_price"::float as underlying_token_price,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    ),
    map_reduce_json as (
        select t1.*
        from flattened_json t1
        left join max_extraction_per_day t2 on t1.date = t2.date
        where t1.extraction_date = t2.extraction_date
    )
    , average_liquidity_rate as (
        select
            block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , min_by(decoded_log:liquidityIndex::float / 1e27, block_timestamp) as first_liquidity_index
            , max_by(decoded_log:liquidityIndex::float / 1e27, block_timestamp) as last_liquidity_index
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where contract_address = lower('{{ contract_address }}')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , liquidity_daily_rate as (
        select
            date
            , reserve
            , first_liquidity_index
            , last_liquidity_index
            , (last_liquidity_index / first_liquidity_index) - 1 as daily_rate
        from average_liquidity_rate
    )
    , data as (
        select 
            map_reduce_json.date
            , reserve
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_rate, 0) as revenue_nominal
            , coalesce(supply_usd * daily_rate, 0) as revenue
            , borrows
            , borrows_usd
        from map_reduce_json
        left join liquidity_daily_rate 
            on map_reduce_json.date = liquidity_daily_rate.date
            and lower(map_reduce_json.underlying_token) = lower(liquidity_daily_rate.reserve)
        order by date 
    )
select 
    date
    , underlying_token as token_address
    , '{{ chain }}' as chain
    , '{{ protocol }}' as app
    , avg(underlying_token_price) as underlying_token_price
    , sum(borrows) as borrows
    , sum(borrows_usd) as borrows_usd
    , sum(supply) as supply
    , sum(supply_usd) as supply_usd
    , sum(revenue) as revenue
    , sum(revenue_nominal) as revenue_nominal
from data
group by 1, 2
{% endmacro %}