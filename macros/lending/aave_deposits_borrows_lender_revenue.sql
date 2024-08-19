{% macro aave_deposits_borrows_lender_revenue(chain, protocol, contract_address, reserve_factor_address, raw_table, healed_raw_table) %}
with
    average_liquidity_rate as (
        select
             block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , avg(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , avg(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where contract_address = lower('{{ contract_address }}')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , reserve_factor_data as (
        select 
            block_timestamp::date as date
            , decoded_log:asset::string as reserve
            , max(decoded_log:newReserveFactor::number) / 1E4 as reserve_factor
        from ethereum_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('{{reserve_factor_address}}')
            and event_name = 'ReserveFactorChanged'
        group by 1, 2
    )
    , daily_rate as (
        select
            date
            , reserve
            , (stable_borrow_rate)/365 as stable_borrow_rate
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
                , LAG(reserve_factor) ignore nulls OVER (partition by reserve ORDER BY date)
                , 0
            ) as reserve_factor
        from average_liquidity_rate
        left join reserve_factor_data using(date, reserve)
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
        from {{ref(raw_table)}} as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
    )
    select 
        data.date
        , data.underlying_token as token_address
        , '{{ chain }}' as chain
        , '{{ protocol }}' as app
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
{% endmacro %}