

with
     __dbt__cte__raw_aave_v3_arbitrum_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
            select *
            from landing_database.prod_landing.raw_aave_v3_lending_arbitrum
            union all
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_arbitrum_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_arbitrum_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from arbitrum_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from arbitrum_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x8145eddDf43f50276641b55bd3AD95944510021E')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from arbitrum_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_arbitrum_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'arbitrum' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v2_avalanche_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v2_avalanche_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v2_avalanche_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from avalanche_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from avalanche_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x230B618aD4C475393A7239aE03630042281BD86e')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from avalanche_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v2_avalanche_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'avalanche' as chain
        , 'AAVE V2' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v3_avalanche_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_avalanche_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_avalanche_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from avalanche_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from avalanche_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x8145eddDf43f50276641b55bd3AD95944510021E')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from avalanche_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_avalanche_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'avalanche' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v3_base_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_base_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_base_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from base_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0xA238Dd80C259a72e81d7e4664a9801593F98d1c5')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from base_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x5731a04B1E775f0fdd454Bf70f3335886e9A96be')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from base_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_base_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'base' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v3_bsc_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_bsc_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_bsc_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from bsc_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x6807dc923806fE8Fd134338EABCA509979a7e0cB')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from bsc_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x67bdF23C7fCE7C65fF7415Ba3F2520B45D6f9584')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from bsc_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_bsc_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'bsc' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v2_ethereum_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
            select *
            from landing_database.prod_landing.raw_aave_v2_lending_ethereum
            union all
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v2_ethereum_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v2_ethereum_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from ethereum_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from ethereum_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x311Bb771e4F8952E6Da169b425E7e92d6Ac45756')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from ethereum_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v2_ethereum_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'ethereum' as chain
        , 'AAVE V2' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v3_ethereum_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
            select *
            from landing_database.prod_landing.raw_aave_v3_lending_ethereum
            union all
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_ethereum_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_ethereum_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from ethereum_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from ethereum_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x64b761D848206f447Fe2dd461b0c635Ec39EbB27')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from ethereum_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_ethereum_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'ethereum' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v3_gnosis_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_gnosis_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_gnosis_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from gnosis_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0xb50201558B00496A145fE76f7424749556E326D8')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from gnosis_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x7304979ec9E4EaA0273b6A037a31c4e9e5A75D16')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from gnosis_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_gnosis_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'gnosis' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v3_optimism_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_optimism_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_optimism_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from optimism_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from optimism_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x8145eddDf43f50276641b55bd3AD95944510021E')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from optimism_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_optimism_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'optimism' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v2_polygon_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
            select *
            from landing_database.prod_landing.raw_aave_v2_lending_polygon
            union all
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v2_polygon_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v2_polygon_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from polygon_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from polygon_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from polygon_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v2_polygon_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'polygon' as chain
        , 'AAVE V2' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__raw_aave_v3_polygon_rpc_data as (



with
    unioned as (
        --Earlier this year aave data was borked. In order to heal the data without doing a complete backfill
        --we pull from the older avve tables. The extract logic is the exact same as the current table (raw_table)
        
            select *
            from landing_database.prod_landing.raw_aave_v3_lending_polygon
            union all
        
        select *
        from LANDING_DATABASE.PROD_LANDING.raw_aave_v3_polygon_borrows_deposits_revenue
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
            flat_json.value:"stable_borrows"::float as stable_borrows,
            flat_json.value:"variable_borrows"::float as variable_borrows,
            flat_json.value:"borrows"::float as borrows,
            flat_json.value:"borrows_usd"::float as borrows_usd,
            flat_json.value:"supply"::float as supply,
            flat_json.value:"supply_usd"::float as supply_usd
        from unioned, lateral flatten(input => parse_json(source_json)) as flat_json
    )
select t1.*
from flattened_json t1
left join max_extraction_per_day t2 on t1.date = t2.date
where t1.extraction_date = t2.extraction_date

),  __dbt__cte__fact_aave_v3_polygon_deposits_borrows_lender_revenue as (



with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from polygon_flipside.core.ez_decoded_event_logs
        where contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(coalesce(decoded_log:newReserveFactor::number, decoded_log:factor::number)) / 1E4 as reserve_factor
            , max(decoded_log:oldReserveFactor::number) / 1E4 as old_reserve_factor
        from polygon_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x8145eddDf43f50276641b55bd3AD95944510021E')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , dates as (
        select distinct block_timestamp::date as date
        from polygon_flipside.core.ez_decoded_event_logs
        where date >= (select min(date) from reserve_factor_data)
    )
    , cross_join_reserve_dates as (
        select 
            reserve
            , date
        from dates
        cross join (
            select distinct reserve
            from reserve_factor_data
        )
    )
    , forward_filled_reserve_factor as (
        select
            date
            , reserve
            , coalesce(
                reserve_factor
                , lag(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as reserve_factor
            , coalesce(
                old_reserve_factor
                , lag(old_reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
            ) as old_reserve_factor
        from cross_join_reserve_dates
        left join reserve_factor_data using(date, reserve)
    )
    , daily_rate as (
        select
            date
            , reserve
            , stable_borrow_rate/365 as stable_borrow_rate
            , (borrow_index /
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(borrow_index) OVER (partition by reserve ORDER BY date)
                    else borrow_index
                end
            ) - 1 as daily_borrow_rate
            , ( liquidity_index / 
                case 
                    when dateadd(day, -1, date) = lag(date) over (partition by reserve order by date)
                    then LAG(liquidity_index) OVER (partition by reserve ORDER BY date)
                    else liquidity_index
                end
            ) - 1 as daily_liquidity_rate
            , coalesce(
                reserve_factor
                , old_reserve_factor
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join forward_filled_reserve_factor using(date, reserve)
    )
    , data as (
        select 
            raw_data.date
            , underlying_token_price
            , underlying_token
            , supply
            , supply_usd
            , coalesce(supply * daily_liquidity_rate, 0) as deposit_revenue_nominal
            , coalesce(deposit_revenue_nominal * underlying_token_price, 0) as deposit_revenue
            , borrows
            , borrows_usd
            , coalesce(variable_borrows * daily_borrow_rate, 0) as variable_borrow_fees_nominal
            , coalesce(stable_borrows * stable_borrow_rate, 0) as stable_borrow_fees_nominal
            , coalesce(variable_borrow_fees_nominal * underlying_token_price, 0) as variable_borrow_fees
            , coalesce(stable_borrow_fees_nominal * underlying_token_price, 0) as stable_borrow_fees
            , coalesce(variable_borrow_fees_nominal, 0) + coalesce(stable_borrow_fees_nominal, 0) as borrow_fees_nominal
            , coalesce(variable_borrow_fees, 0) + coalesce(stable_borrow_fees, 0) as borrow_fees
            , borrow_fees_nominal * coalesce(reserve_factor, 0) as reserve_factor_revenue_nominal
            , borrow_fees * coalesce(reserve_factor, 0) as reserve_factor_revenue
        from __dbt__cte__raw_aave_v3_polygon_rpc_data as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , 'polygon' as chain
        , 'AAVE V3' as app
        , avg(underlying_token_price) as underlying_token_price
        , sum(borrows) as borrows
        , sum(borrows_usd) as borrows_usd
        , sum(supply) as supply
        , sum(supply_usd) as supply_usd
        , sum(deposit_revenue) as deposit_revenue
        , sum(deposit_revenue_nominal) as deposit_revenue_nominal
        , sum(variable_borrow_fees) as variable_borrow_fees
        , sum(variable_borrow_fees_nominal) as variable_borrow_fees_nominal
        , sum(stable_borrow_fees) as stable_borrow_fees
        , sum(stable_borrow_fees_nominal) as stable_borrow_fees_nominal
        , sum(borrow_fees_nominal) as interest_rate_fees_nominal
        , sum(borrow_fees) as interest_rate_fees
        , sum(reserve_factor_revenue_nominal) as reserve_factor_revenue_nominal
        , sum(reserve_factor_revenue) as reserve_factor_revenue
    from data
    group by 1, 2

),  __dbt__cte__fact_aave_v3_arbitrum_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'arbitrum' as chain
    , 'Aave V3' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from arbitrum_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V3'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v2_avalanche_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'avalanche' as chain
    , 'Aave V2' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from avalanche_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V2'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v3_avalanche_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'avalanche' as chain
    , 'Aave V3' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from avalanche_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V3'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v3_base_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'base' as chain
    , 'Aave V3' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from base_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V3'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v2_ethereum_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'ethereum' as chain
    , 'Aave V2' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from ethereum_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V2'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v3_ethereum_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'ethereum' as chain
    , 'Aave V3' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from ethereum_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V3'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v3_gnosis_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'gnosis' as chain
    , 'Aave V3' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from gnosis_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V3'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v3_optimism_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'optimism' as chain
    , 'Aave V3' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from optimism_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V3'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v2_polygon_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'polygon' as chain
    , 'Aave V2' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from polygon_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V2'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v3_polygon_flashloan_fees as (




select 
    block_timestamp::date as date
    , 'polygon' as chain
    , 'Aave V3' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from polygon_flipside.defi.ez_lending_flashloans 
where platform = 'Aave V3'
group by 1, 2, 3, 4

),  __dbt__cte__fact_aave_v3_arbitrum_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from arbitrum_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
    
)
select
    block_timestamp::date as date
    , 'arbitrum' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join arbitrum_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join arbitrum_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v2_avalanche_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from avalanche_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')
    
)
select
    block_timestamp::date as date
    , 'avalanche' as chain
    , 'Aave V2' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join avalanche_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join avalanche_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_avalanche_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from avalanche_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
    
)
select
    block_timestamp::date as date
    , 'avalanche' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join avalanche_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join avalanche_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_base_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from base_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0xA238Dd80C259a72e81d7e4664a9801593F98d1c5')
    
)
select
    block_timestamp::date as date
    , 'base' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join base_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join base_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_bsc_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from bsc_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x6807dc923806fE8Fd134338EABCA509979a7e0cB')
    
)
select
    block_timestamp::date as date
    , 'bsc' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join bsc_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join bsc_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v2_ethereum_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from ethereum_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')
    
)
select
    block_timestamp::date as date
    , 'ethereum' as chain
    , 'Aave V2' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join ethereum_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join ethereum_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_ethereum_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from ethereum_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2')
    
)
select
    block_timestamp::date as date
    , 'ethereum' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join ethereum_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join ethereum_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_gnosis_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from gnosis_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0xb50201558B00496A145fE76f7424749556E326D8')
    
)
select
    block_timestamp::date as date
    , 'gnosis' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join gnosis_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join gnosis_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_optimism_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from optimism_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
    
)
select
    block_timestamp::date as date
    , 'optimism' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join optimism_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join optimism_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v2_polygon_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from polygon_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')
    
)
select
    block_timestamp::date as date
    , 'polygon' as chain
    , 'Aave V2' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join polygon_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join polygon_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_polygon_liquidation_revenue as (



with
liquidator_events as (
    select 
        block_timestamp
        , tx_hash
        , event_index
        , decoded_log:liquidator::string as liquidator
        , decoded_log:user::string as user
        , coalesce(decoded_log:collateralAsset::string, decoded_log:collateral::string) as collateral_asset
        , decoded_log:liquidatedCollateralAmount::float as liquidated_collateral_amount
        , coalesce(decoded_log:debtAsset::string, decoded_log:principal::string) as debt_asset
        , decoded_log:debtToCover::float as debt_to_cover
    from polygon_flipside.core.ez_decoded_event_logs 
    where event_name = 'LiquidationCall'
        and contract_address = lower('0x794a61358D6845594F94dc1DB02A252b5b4814aD')
    
)
select
    block_timestamp::date as date
    , 'polygon' as chain
    , 'Aave V3' as protocol
    , block_timestamp
    , tx_hash
    , event_index
    , collateral_asset
    , liquidated_collateral_amount/pow(10, collateral_price.decimals) as collateral_amount_nominal
    , collateral_amount_nominal * collateral_price.price as collateral_amount_usd
    , debt_asset
    , debt_to_cover / pow(10, debt_price.decimals) as debt_amount_nominal
    , debt_amount_nominal * debt_price.price as debt_amount_usd
    , collateral_amount_usd - debt_amount_usd as liquidation_revenue
from liquidator_events
left join polygon_flipside.price.ez_prices_hourly collateral_price
    on lower(collateral_asset) = lower(collateral_price.token_address)
        and date_trunc(hour, block_timestamp) = hour
left join polygon_flipside.price.ez_prices_hourly debt_price
    on lower(debt_asset) = lower(debt_price.token_address)
        and date_trunc(hour, block_timestamp) = debt_price.hour

),  __dbt__cte__fact_aave_v3_arbitrum_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from arbitrum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x929EC64c34a17401F460460D4B9390518E5B473e')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join arbitrum_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'arbitrum' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v2_avalanche_ecosystem_incentives as (



with
event_logs as (
    select 
        block_timestamp
        , case 
            when 'avalanche' = 'etherum' then '0x4da27a545c0c5B758a6BA100e3a049001de870f5' 
            when 'avalanche' = 'avalanche' then '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
            else '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
        end as asset
        , decoded_log:amount::float as amount
    from ethereum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x01D83Fe6A10D2f2B7AF17034343746188272cAc9')
        and event_name = 'RewardsClaimed'
)
, prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'aave'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'aave'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset as token_address
        , amount
        , amount / 1E18 as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join prices on block_timestamp::date = date
)
select
    date
    , 'avalanche' as chain
    , 'AAVE V2' as protocol
    , token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v3_avalanche_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from avalanche_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x929EC64c34a17401F460460D4B9390518E5B473e')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join avalanche_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'avalanche' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v3_base_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from base_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0xf9cc4F0D883F1a1eb2c253bdb46c254Ca51E1F44')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join base_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'base' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v3_bsc_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from bsc_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0xC206C2764A9dBF27d599613b8F9A63ACd1160ab4')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join bsc_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'bsc' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v2_ethereum_ecosystem_incentives as (



with
event_logs as (
    select 
        block_timestamp
        , case 
            when 'ethereum' = 'etherum' then '0x4da27a545c0c5B758a6BA100e3a049001de870f5' 
            when 'ethereum' = 'avalanche' then '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
            else '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
        end as asset
        , decoded_log:amount::float as amount
    from ethereum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0xd784927Ff2f95ba542BfC824c8a8a98F3495f6b5')
        and event_name = 'RewardsClaimed'
)
, prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'aave'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'aave'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset as token_address
        , amount
        , amount / 1E18 as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join prices on block_timestamp::date = date
)
select
    date
    , 'ethereum' as chain
    , 'AAVE V2' as protocol
    , token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v3_ethereum_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from ethereum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x8164Cc65827dcFe994AB23944CBC90e0aa80bFcb')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join ethereum_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'ethereum' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v3_gnosis_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from gnosis_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0xaD4F91D26254B6B0C6346b390dDA2991FDE2F20d')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join gnosis_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'gnosis' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v3_optimism_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from optimism_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x929EC64c34a17401F460460D4B9390518E5B473e')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join optimism_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'optimism' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v2_polygon_ecosystem_incentives as (



with
event_logs as (
    select 
        block_timestamp
        , case 
            when 'polygon' = 'etherum' then '0x4da27a545c0c5B758a6BA100e3a049001de870f5' 
            when 'polygon' = 'avalanche' then '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
            else '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
        end as asset
        , decoded_log:amount::float as amount
    from ethereum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x357D51124f59836DeD84c8a1730D72B749d8BC23')
        and event_name = 'RewardsClaimed'
)
, prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'aave'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'aave'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset as token_address
        , amount
        , amount / 1E18 as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join prices on block_timestamp::date = date
)
select
    date
    , 'polygon' as chain
    , 'AAVE V2' as protocol
    , token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_v3_polygon_ecosystem_incentives as (



with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from polygon_flipside.core.ez_decoded_event_logs
    where contract_address = lower('0x929EC64c34a17401F460460D4B9390518E5B473e')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join polygon_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'polygon' as chain
    , 'AAVE V3' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4

),  __dbt__cte__fact_aave_aavura_treasury as (



WITH 
tokens as (
    SELECT LOWER(address) AS address
    FROM (
        VALUES
        ('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'),
        ('0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f'),
        ('0xdAC17F958D2ee523a2206206994597C13D831ec7'),
        ('0x5aFE3855358E112B5647B952709E6165e1c1eEEe'),
        ('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
        ('0x6B175474E89094C44Da98b954EedeAC495271d0F')
    ) AS addresses(address)
)
, base AS (
    select
        to_address,
        from_address,
        contract_address,
        block_timestamp::date as date,
        amount_precise,
        min(block_timestamp::date) OVER() as min_date
    FROM ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) in (select address from tokens)
)
,  date_range AS (
    SELECT *
        FROM (
            SELECT
                min_date + SEQ4() AS date
            FROM base
        )
    WHERE date <= TO_DATE(SYSDATE())
)
, flows as (
    SELECT
        date,
        contract_address,
        SUM(CASE WHEN to_address = lower('0x89C51828427F70D77875C6747759fB17Ba10Ceb0') THEN amount_precise ELSE 0 END) AS amount_in,
        SUM(CASE WHEN from_address = lower('0x89C51828427F70D77875C6747759fB17Ba10Ceb0') THEN amount_precise ELSE 0 END) AS amount_out
    FROM base
    GROUP BY 1, 2
    ORDER BY 1 DESC
)
, prices as (
    select
        hour::date as date
        , token_address
        , avg(price) as price
    from ethereum_flipside.price.ez_prices_hourly
    where token_address in (select address from tokens)
    group by 1, 2
)

SELECT
    dr.date AS date
    , 'ethereum' as chain
    , contract_address as token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (partition by contract_address ORDER BY dr.date) as amount_nominal
    , amount_nominal * p.price as amount_usd
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p 
    on p.date = dr.date 
    and lower(p.token_address) = lower(f.contract_address)
ORDER BY date DESC
),  __dbt__cte__fact_aave_v2_collector as (



WITH 

tokens as (
    SELECT LOWER(a_token) AS a_token, LOWER(priced_token) AS priced_token
    FROM (
        VALUES
        ('0x98C23E9d8f34FEFb1B7BD6a91B7FF122F4e16F5c', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
        ('0x23878914EFE38d27C4D67Ab83ed1b93A74D4086a', '0xdAC17F958D2ee523a2206206994597C13D831ec7'),
        ('0xae78736Cd615f374D3085123A210448E74Fc6393', '0xae78736Cd615f374D3085123A210448E74Fc6393'),
        ('0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f', '0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f'),
        ('0x4d5F47FA6A74757f35C14fD3a6Ef8E3C9BC514E8', '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),
        ('0xBcca60bB61934080951369a648Fb03DF4F96263C', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
        ('0x3Ed3B47Dd13EC9a98b44e6204A523E766B225811', '0xdAC17F958D2ee523a2206206994597C13D831ec7'),
        ('0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0', '0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'),
        ('0x5Ee5bf7ae06D1Be5997A1A72006FE6C607eC6DE8', '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'),
        ('0x018008bfb33d285247A21d44E50697654f754e63', '0x6B175474E89094C44Da98b954EedeAC495271d0F'),
        ('0x028171bCA77440897B824Ca71D1c56caC55b68A3', '0x6B175474E89094C44Da98b954EedeAC495271d0F'),
        ('0x0B925eD163218f6662a35e0f0371Ac234f9E9371', '0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'),
        ('0x030bA81f1c18d280636F32af80b9AAd02Cf0854e', '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),
        ('0x5E8C8A7243651DB1384C0dDfDbE39761E8e7E51a', '0x514910771AF9Ca656af840dff83E8264EcF986CA'),
        ('0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0', '0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0'),
        ('0x6B175474E89094C44Da98b954EedeAC495271d0F', '0x6B175474E89094C44Da98b954EedeAC495271d0F'),
        ('0x101cc05f4A51C0319f570d5E146a8C625198e636', '0x0000000000085d4780B73119b644AE5ecd22b376'),
        ('0xA361718326c15715591c299427c62086F69923D9', '0x4Fabb145d64652a948d72533023f6E7A623C7C53'),
        ('0xC7B4c17861357B8ABB91F25581E7263E08DCB59c', '0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F'),
        ('0xD37EE7e4f452C6638c96536e68090De8cBcdb583', '0xa0E5A19E091BBe241E655997E50da82DA676b083'),
        ('0xA700b4eB416Be35b2911fd5Dee80678ff64fF6C9', '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'),
        ('0x9ff58f4fFB29fA2266Ab25e75e2A8b3503311656', '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'),
        ('0x6C5024Cd4F8A59110119C56f8933403A539555EB', '0x57Ab1ec28D129707052df4dF418D58a2D46d5f51'),
        ('0xCc9EE9483f662091a1de4795249E24aC0aC2630f', '0xae78736Cd615f374D3085123A210448E74Fc6393'),
        ('0x3Fe6a295459FAe07DF8A0ceCC36F37160FE86AA9', '0x5f98805A4E8be255a32880FDeC7F6728C6568bA0'),
        ('0xB76CF92076adBF1D9C39294FA8e7A67579FDe357', '0xD33526068D116cE69F19A9ee46F0bd304F21A51f'),
        ('0x8A458A9dc9048e005d22849F470891b840296619', '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
        ('0xF6D2224916DDFbbab6e6bd0D1B7034f4Ae0CaB18', '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'),
        ('0x977b6fc5dE62598B08C85AC8Cf2b745874E8b78c', '0xBe9895146f7AF43049ca1c1AE358B0541Ea49704'),
        ('0x1494CA1F11D487c2bBe4543E90080AeBa4BA3C2b', '0x1494CA1F11D487c2bBe4543E90080AeBa4BA3C2b'),
        ('0xd4e245848d6E1220DBE62e155d89fa327E43CB06', '0x853d955aCEf822Db058eb8505911ED77F175b99e'),
        ('0xc9BC48c72154ef3e5425641a3c747242112a46AF', '0x03ab458634910AaD20eF5f1C8ee96F1D6ac54919'),
        ('0xc713e5E149D5D0715DcD1c156a020976e7E56B88', '0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2'),
        ('0x7B95Ec873268a6BFC6427e7a28e396Db9D0ebc65', '0xD533a949740bb3306d119CC777fa900bA034cd52'),
        ('0xa685a61171bb30d4072B338c80Cb7b2c865c873E', '0x0F5D2fB29fb7d3CFeE444a200298f468908cC942'),
        ('0xB9D7CB55f463405CDfBe4E90a6D2Df01C2B92BF1', '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'),
        ('0xd24946147829DEaA935bE2aD85A3291dbf109c80', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),
        ('0x39C6b3e42d6A679d7D776778Fe880BC9487C2EDA', '0xdd974D5C2e2928deA5F71b9825b8b646686BD200'),
        ('0x2516E7B3F76294e03C42AA4c5b5b4DCE9C436fB8', '0xba100000625a3754423978a60c9317c58a424e3D'),
        ('0x514910771AF9Ca656af840dff83E8264EcF986CA', '0x514910771AF9Ca656af840dff83E8264EcF986CA'),
        ('0x9A44fd41566876A39655f74971a3A6eA0a17a454', '0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32'),
        ('0x71Aef7b30728b9BB371578f36c5A1f1502a5723e', '0x111111111117dC0aa78b770fA6A738034120C302'),
        ('0xaC6Df26a590F08dcC95D5a4705ae8abbc88509Ef', '0xF629cBd94d3791C9250152BD8dfBDF380E2a3B9c'),
        ('0x79bE75FFC64DD58e66787E4Eae470c8a1FD08ba4', '0x6B175474E89094C44Da98b954EedeAC495271d0F'),
        ('0xDf7FF54aAcAcbFf42dfe29DD6144A69b629f8C9e', '0xE41d2489571d322189246DaFA5ebDe1F4699F498'),
        ('0x272F97b7a56a387aE942350bBC7Df5700f8a4576', '0xba100000625a3754423978a60c9317c58a424e3D'),
        ('0x05Ec93c0365baAeAbF7AefFb0972ea7ECdD39CF1', '0x0D8775F648430679A709E98d2b0Cb6250d2887EF'),
        ('0x13B2f6928D7204328b0E8E4BCd0379aA06EA21FA', '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'),
        ('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9', '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
    ) AS t(a_token, priced_token)
)
, base AS (
    select
        to_address,
        from_address,
        contract_address,
        priced_token,
        block_timestamp::date as date,
        amount,
        min(block_timestamp::date) OVER() as min_date
    FROM ethereum_flipside.core.ez_token_transfers
    inner join tokens on lower(contract_address) = lower(a_token)
)
,  date_range AS (
    SELECT *
        FROM (
            SELECT
                min_date + SEQ4() AS date
            FROM base
        )
    WHERE date <= TO_DATE(SYSDATE())
)
, flows as (
    SELECT
        date,
        contract_address,
        priced_token,
        SUM(CASE WHEN to_address = lower('0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c') THEN amount ELSE 0 END) AS amount_in,
        SUM(CASE WHEN from_address = lower('0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c') THEN amount ELSE 0 END) AS amount_out
    FROM base
    GROUP BY 1, 2, 3
)
, prices as (
    select
        hour::date as date
        , token_address
        , avg(price) as price
    from ethereum_flipside.price.ez_prices_hourly
    where token_address in (select distinct priced_token from tokens)
    group by 1, 2
)

SELECT
    dr.date AS date
    , 'ethereum' as chain
    , contract_address as token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (partition by contract_address ORDER BY dr.date) as amount_nominal
    , amount_nominal * p.price as amount_usd
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p 
    on p.date = dr.date 
    and lower(p.token_address) = lower(f.priced_token)
),  __dbt__cte__fact_aave_safety_module as (






  -- Add 1 to include both start and end dates

with
stkAAVE as (
    select
        block_timestamp::date as date
        , lower('0x4da27a545c0c5b758a6ba100e3a049001de870f5') as token
        , case 
            when to_address = '0x0000000000000000000000000000000000000000' then -amount
            when from_address = '0x0000000000000000000000000000000000000000' then amount
        end as mint
    from ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x4da27a545c0c5b758a6ba100e3a049001de870f5')
        and (
            to_address = '0x0000000000000000000000000000000000000000'
            or from_address = '0x0000000000000000000000000000000000000000'
        )
)
, stkABPT_mints as (
    select
        block_timestamp::date as date
        , lower('0xa1116930326d21fb917d5a27f1e9943a9595fb47') as token
        , case 
            when to_address = '0x0000000000000000000000000000000000000000' then -amount
            when from_address = '0x0000000000000000000000000000000000000000' then amount
        end as mint
    from ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0xa1116930326d21fb917d5a27f1e9943a9595fb47')
        and (
            to_address = '0x0000000000000000000000000000000000000000'
            or from_address = '0x0000000000000000000000000000000000000000'
        )
)
, stkGHO_mints as (
    select
        block_timestamp::date as date
        , lower('0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d') as token
        , case 
            when to_address = '0x0000000000000000000000000000000000000000' then -amount
            when from_address = '0x0000000000000000000000000000000000000000' then amount
        end as mint
    from ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d')
        and (
            to_address = '0x0000000000000000000000000000000000000000'
            or from_address = '0x0000000000000000000000000000000000000000'
        )
)
, tokens as (
    SELECT lower('0x4da27a545c0c5b758a6ba100e3a049001de870f5') as token 
    UNION 
    SELECT lower('0xa1116930326d21fb917d5a27f1e9943a9595fb47') as token
    UNION 
    SELECT lower('0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d') as token
)
, dt_spine as (
    SELECT '2018-01-01'::date + seq4() AS date
    FROM TABLE(GENERATOR(ROWCOUNT => 2427))
    where date <= to_date(sysdate())
)
, token_days as (
    SELECT tokens.token, dt_spine.date
    from tokens 
    CROSS JOIN dt_spine 
)
, daily_mint as (
    SELECT 
        date
        , token
        , sum(mint) as daily_mint
    FROM (
        SELECT * FROM stkAAVE
        UNION ALL 
        SELECT * FROM stkABPT_mints
        UNION ALL 
        SELECT * FROM stkGHO_mints
    ) a
    GROUP BY date, token
)
, daily_mints_filled as (
    SELECT 
        token_days.token
        , token_days.date
        , COALESCE(daily_mint.daily_mint, 0) as daily_mint
    from token_days 
    LEFT JOIN daily_mint
        ON daily_mint.date = token_days.date
        AND lower(daily_mint.token) = lower(token_days.token)
)
, result as (
    SELECT 
        token
        , date
        , daily_mint
        , sum(daily_mint) over(partition by token order by date) as total_supply
    FROM daily_mints_filled
)
, aave_prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'aave'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'aave'
)
, gho_prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'gho'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'gho'
)
, abpt_prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'aave-balancer-pool-token'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'aave-balancer-pool-token'
)
, prices as (
    select date, '0x4da27a545c0c5b758a6ba100e3a049001de870f5' as token, price 
    from aave_prices
        
    union all 

    select date, '0xa1116930326d21fb917d5a27f1e9943a9595fb47' as token, price 
    from abpt_prices

    union all 

    select date, '0x1a88df1cfe15af22b3c4c783d4e6f7f9e0c1885d' as token, price 
    from gho_prices
)


SELECT 
    result.date
    , 'ethereum' as chain
    , result.token as token_address
    , total_supply as amount_nominal
    , coalesce(prices.price, 0) * total_supply as amount_usd
FROM result
LEFT JOIN prices
    ON prices.token = result.token
    AND prices.date = result.date
),  __dbt__cte__fact_aave_ecosystem_reserve as (



WITH 
base AS (
    select
        to_address,
        from_address,
        block_timestamp::date as date,
        amount_precise,
        min(block_timestamp::date) OVER() as min_date
    FROM ethereum_flipside.core.ez_token_transfers
    where lower(contract_address) = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
)
,  date_range AS (
    SELECT *
        FROM (
            SELECT
                min_date + SEQ4() AS date
            FROM base
        )
    WHERE date <= TO_DATE(SYSDATE())
)
, flows as (
    SELECT
        date,
        SUM(CASE WHEN to_address = lower('0x25F2226B597E8F9514B3F68F00f494cF4f286491') THEN amount_precise ELSE 0 END) AS amount_in,
        SUM(CASE WHEN from_address = lower('0x25F2226B597E8F9514B3F68F00f494cF4f286491') THEN amount_precise ELSE 0 END) AS amount_out
    FROM base
    GROUP BY 1
    ORDER BY 1 DESC
)
, prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'aave'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'aave'
)

SELECT
    dr.date AS date
    , 'ethereum' as chain
    , '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9' as token_address
    , SUM(COALESCE(f.amount_in, 0) - COALESCE(f.amount_out, 0)) OVER (ORDER BY dr.date) as amount_nominal
    , amount_nominal * p.price as amount_usd
FROM date_range dr
LEFT JOIN flows f
    ON f.date = dr.date
LEFT JOIN prices p on p.date = dr.date
ORDER BY date DESC
),  __dbt__cte__fact_aave_dao_balancer_trading_fees as (


with
swaps as (
    select 
        block_timestamp
        , decoded_log:tokenIn::string as token_address
        , decoded_log:tokenAmountIn::float * 0.001 as amount
    from ethereum_flipside.core.ez_decoded_event_logs 
    where contract_address = lower('0xC697051d1C6296C24aE3bceF39acA743861D9A81') 
        and event_name = 'LOG_SWAP'
)
, swap_revenue as (
    select
        block_timestamp::date as date
        , swaps.token_address
        , coalesce(amount / pow(10, decimals), 0) as amount_nominal
        , coalesce(amount_nominal * price, 0) as amount_usd
    from swaps
    left join ethereum_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour 
        and lower(swaps.token_address) = lower(p.token_address)
)
select
    date
    , token_address
    , 'AAVE DAO' as protocol
    , 'ethereum' as chain
    , sum(coalesce(amount_nominal, 0)) as trading_fees_nominal
    , sum(coalesce(amount_usd, 0)) as trading_fees_usd
from swap_revenue 
where date < to_date(sysdate())
group by 1, 2
order by 1 desc
),  __dbt__cte__fact_aave_dao_safety_incentives as (


with 
    logs as (
        select 
            block_timestamp
            , decoded_log:amount::float / 1E18 as amount_nominal
        from ethereum_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x4da27a545c0c5B758a6BA100e3a049001de870f5')
            and event_name = 'RewardsClaimed'
    )
    , prices as (
    select date as date, shifted_token_price_usd as price
    from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'aave'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from PC_DBT_DB.PROD.fact_coingecko_token_realtime_data
    where token_id = 'aave'
)
    , priced_logs as (
        select
            block_timestamp::date as date
            , '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9' as token_address
            , amount_nominal
            , amount_nominal * price as amount_usd
        from logs
        left join prices on block_timestamp::date = date
    )
select
    date
    , token_address
    , 'AAVE DAO' as protocol
    , 'ethereum' as chain
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from priced_logs
group by 1, 2
),  __dbt__cte__fact_aave_gho_treasury_revenue as (


with
event_logs as (
    select 
        block_timestamp
        , '0x' || substr(topics[2]::string, 27, 40) as asset
        , pc_dbt_db.prod.hex_to_int(data) as amount
    from ethereum_flipside.core.fact_event_logs 
    where contract_address = lower('0x00907f9921424583e7ffBfEdf84F92B7B2Be4977')
        and topics[0]::string = '0xb29fcda740927812f5a71075b62e132bead3769a455319c29b9a1cc461a65475'
)
, priced_logs as (
    select
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from event_logs
    left join ethereum_flipside.price.ez_prices_hourly
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , 'AAVE GHO' as protocol
    , 'ethereum' as chain
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from priced_logs
group by 1, 4
order by 1 desc
), deposits_borrows_lender_revenue as (
        select * from __dbt__cte__fact_aave_v3_arbitrum_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v2_avalanche_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_avalanche_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_base_deposits_borrows_lender_revenue
        union all 
        select * from __dbt__cte__fact_aave_v3_bsc_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v2_ethereum_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_ethereum_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_gnosis_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_optimism_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v2_polygon_deposits_borrows_lender_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_polygon_deposits_borrows_lender_revenue
    )
    , aave_outstanding_supply_net_deposits_deposit_revenue as (
        select
            date
            , chain
            , sum(borrows_usd) as outstanding_supply
            , sum(supply_usd) as net_deposits
            , net_deposits - outstanding_supply as tvl
            , sum(deposit_revenue) as supply_side_deposit_revenue
            , sum(interest_rate_fees) as interest_rate_fees
            , sum(reserve_factor_revenue) as reserve_factor_revenue
        from deposits_borrows_lender_revenue
        group by 1, 2
    )
    , flashloan_fees as (
        select * from __dbt__cte__fact_aave_v3_arbitrum_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v2_avalanche_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v3_avalanche_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v3_base_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v2_ethereum_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v3_ethereum_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v3_gnosis_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v3_optimism_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v2_polygon_flashloan_fees
        union all
        select * from __dbt__cte__fact_aave_v3_polygon_flashloan_fees
    )
    , aave_flashloan_fees as (
        select 
            date
            , chain
            , sum(amount_usd) as flashloan_fees
        from flashloan_fees
        group by 1, 2
    )
    , liquidation_revenue as (
        select * from __dbt__cte__fact_aave_v3_arbitrum_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v2_avalanche_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_avalanche_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_base_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_bsc_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v2_ethereum_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_ethereum_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_gnosis_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_optimism_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v2_polygon_liquidation_revenue
        union all
        select * from __dbt__cte__fact_aave_v3_polygon_liquidation_revenue
    )
    , aave_liquidation_supply_side_revenue as (
        select 
            date
            , chain
            , sum(liquidation_revenue) as liquidation_revenue
        from liquidation_revenue
        group by 1, 2
    )
    , ecosystem_incentives as (
        select * from __dbt__cte__fact_aave_v3_arbitrum_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v2_avalanche_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v3_avalanche_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v3_base_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v3_bsc_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v2_ethereum_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v3_ethereum_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v3_gnosis_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v3_optimism_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v2_polygon_ecosystem_incentives
        union all
        select * from __dbt__cte__fact_aave_v3_polygon_ecosystem_incentives
    )
    , aave_treasury as (
        select * from __dbt__cte__fact_aave_aavura_treasury
        union all
        select * from __dbt__cte__fact_aave_v2_collector
        union all
        select * from __dbt__cte__fact_aave_safety_module
        union all
        select * from __dbt__cte__fact_aave_ecosystem_reserve
    )
    , treasury as (
        select
            date
            , chain
            , sum(case when token_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') then amount_usd else 0 end) as treasury_value_native
            , sum(amount_usd) as treasury_value
        from aave_treasury
        group by date, 2
    )
    , aave_net_treasury as (
        select * from __dbt__cte__fact_aave_v2_collector
        union all
        select * from __dbt__cte__fact_aave_aavura_treasury
    )
    , net_treasury_data as (
        select
            date
            , chain
            , sum(amount_usd) as net_treasury_value
        from aave_net_treasury
        group by 1, 2
    )
    , aave_ecosystem_incentives as (
        select 
            date
            , chain
            , sum(amount_usd) as ecosystem_incentives
        from ecosystem_incentives
        group by 1, 2
    )
    , dao_trading_revenue as (
        select
            date
            , chain
            , sum(trading_fees_usd) as trading_fees
        from __dbt__cte__fact_aave_dao_balancer_trading_fees
        group by 1, 2
    )
    , safety_incentives as (
        select
            date
            , chain
            , sum(amount_usd) as safety_incentives
        from __dbt__cte__fact_aave_dao_safety_incentives
        group by 1, 2
    )
    , gho_treasury_revenue as (
        select
            date
            , chain
            , sum(amount_usd) as gho_revenue
        from __dbt__cte__fact_aave_gho_treasury_revenue
        group by 1, 2
    )
   
select
    aave_outstanding_supply_net_deposits_deposit_revenue.date
    , chain
    , coalesce(interest_rate_fees, 0) as interest_rate_fees
    , flashloan_fees
    , gho_revenue as gho_fees
    , coalesce(interest_rate_fees, 0) + coalesce(flashloan_fees, 0) + coalesce(gho_fees, 0) as fees
    , supply_side_deposit_revenue
    , coalesce(supply_side_deposit_revenue, 0) as primary_supply_side_revenue
    , flashloan_fees as flashloan_supply_side_revenue
    , liquidation_revenue as liquidation_supply_side_revenue
    , ecosystem_incentives as ecosystem_supply_side_revenue
    , coalesce(flashloan_fees, 0) + coalesce(gho_revenue, 0) + coalesce(liquidation_revenue, 0) + coalesce(ecosystem_incentives, 0) as secondary_supply_side_revenue
    , primary_supply_side_revenue + secondary_supply_side_revenue as total_supply_side_revenue
    , trading_fees as dao_trading_revenue
    , gho_revenue
    , coalesce(reserve_factor_revenue, 0) as reserve_factor_revenue
    , coalesce(reserve_factor_revenue, 0) + coalesce(dao_trading_revenue, 0) + coalesce(gho_revenue, 0) as protocol_revenue
    , ecosystem_incentives
    , safety_incentives
    , coalesce(ecosystem_incentives, 0) + coalesce(safety_incentives, 0) as token_incentives
    , token_incentives as total_expenses 
    , coalesce(protocol_revenue, 0) - coalesce(total_expenses, 0) as protocol_earnings
    , outstanding_supply
    , net_deposits
    , tvl
    , treasury_value
    , net_treasury_value
    , treasury_value_native
from aave_outstanding_supply_net_deposits_deposit_revenue
left join aave_flashloan_fees using (date, chain)
left join aave_liquidation_supply_side_revenue using (date, chain)
left join aave_ecosystem_incentives using (date, chain)
left join dao_trading_revenue using (date, chain)
left join safety_incentives using (date, chain)
left join gho_treasury_revenue using (date, chain)
left join treasury using (date, chain)
left join net_treasury_data using (date, chain)
where aave_outstanding_supply_net_deposits_deposit_revenue.date < to_date(sysdate())
