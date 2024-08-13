{% macro aave_deposits_borrows_lender_revenue(chain, protocol, contract_address, raw_table, healed_raw_table) %}
with
    average_liquidity_rate as (
        select
            block_timestamp::date as date
            , decoded_log:reserve::string as reserve
            , max(decoded_log:stableBorrowRate::float / 1e27) as stable_borrow_rate
            , avg(decoded_log:variableBorrowIndex::float / 1e27) as borrow_index
            , min(decoded_log:liquidityIndex::float / 1e27) as liquidity_index
            -- , min_by(decoded_log:variableBorrowIndex::float / 1e27, block_timestamp) as borrow_index
            -- , min_by(decoded_log:liquidityIndex::float / 1e27, block_timestamp) as liquidity_index
            -- , min_by(decoded_log:variableBorrowIndex::float / 1e27, block_timestamp) as first_borrow_index
            -- , max_by(decoded_log:variableBorrowIndex::float / 1e27, block_timestamp) as last_borrow_index
            -- , min_by(decoded_log:liquidityIndex::float / 1e27, block_timestamp) as first_liquidity_index
            -- , max_by(decoded_log:liquidityIndex::float / 1e27, block_timestamp) as last_liquidity_index
        from {{ chain }}_flipside.core.ez_decoded_event_logs
        where contract_address = lower('{{ contract_address }}')
            and event_name = 'ReserveDataUpdated'
        group by 1, 2
    )
    , daily_rate as (
        select
            date
            , reserve
            , 1 + (stable_borrow_rate/365) as stable_borrow_rate
            , (borrow_index / coalesce(LAG(borrow_index) IGNORE NULLS OVER (partition by reserve ORDER BY date), borrow_index)) - 1 as daily_borrow_rate
            , (liquidity_index / coalesce(LAG(liquidity_index) IGNORE NULLS OVER (partition by reserve ORDER BY date), liquidity_index)) - 1 as daily_liquidity_rate

            -- , (last_borrow_index / first_borrow_index) - 1 as daily_borrow_rate
            -- , (last_liquidity_index / first_liquidity_index) - 1 as daily_liquidity_rate
        from average_liquidity_rate
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
        from {{ref(raw_table)}} as raw_data
        left join daily_rate 
            on raw_data.date = daily_rate.date
            and lower(raw_data.underlying_token) = lower(daily_rate.reserve)
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
    , sum(deposit_revenue) as deposit_revenue
    , sum(deposit_revenue_nominal) as deposit_revenue_nominal
    , sum(borrow_fees_nominal) as interest_rate_fees_nominal
    , sum(borrow_fees) as interest_rate_fees
from data
group by 1, 2
{% endmacro %}