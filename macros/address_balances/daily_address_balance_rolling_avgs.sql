{% macro daily_address_balance_rolling_avgs(chain) %}

    with balances as (
        select   
            date 
            , address 
            , balance_usd
            , native_token_balance
            , stablecoin_balance
        from {{ ref("fact_" ~ chain ~ "_daily_balances")}}
        {% if is_incremental() %}
            -- only want to scan data for addresses whose balance changed recently in last 3 days
            where address in (
                select address 
                from {{ ref("fact_" ~ chain ~ "_daily_balances")}} 
                where date >= coalesce(
                    (select dateadd('day', -3, max(date)) from {{ this }}),
                    '2023-01-01'
                )
            )
            -- need to then scan last 180 days of balance changes for these select addresses since largest rolling avg is 180d
            and date >= coalesce(
                (select dateadd('day', -180, max(date)) from {{ this }}),
                '2023-01-01'
            )
        {% endif %}
    )

    , date_spine as (
        select date 
        from {{ ref('dim_date_spine') }}
        where date between (select min(date) from balances) and current_date
    )

    , addresses_per_date as (
        select  
            ds.date 
            , bpa.address
        from date_spine ds 
        cross join balances bpa
        group by all 
    )

    , addresses_with_balance_rolling_avgs AS (
        with balances_joined as (
            select 
                apd.date
                , apd.address
                , b.balance_usd
                , b.native_token_balance
                , b.stablecoin_balance
            from addresses_per_date apd 
            left join balances b
                on apd.date = b.date 
                and apd.address = b.address
        )

        , last_values_carried_forward as (
            select 
                date
                , address
                , coalesce(last_value(balance_usd ignore nulls) over (
                    partition by address 
                    order by date 
                    rows between unbounded preceding and current row
                ), 0) as balance_usd
                , coalesce(last_value(native_token_balance ignore nulls) over (
                    partition by address 
                    order by date 
                    rows between unbounded preceding and current row
                ), 0) as native_token_balance
                , coalesce(last_value(stablecoin_balance ignore nulls) over (
                    partition by address 
                    order by date 
                    rows between unbounded preceding and current row
                ), 0) as stablecoin_balance
            from balances_joined
        )

        select 
            date 
            , address
            , balance_usd
            , avg(balance_usd) over (partition by address order by date rows between 30 preceding and current row) as balance_usd_30d_avg
            , avg(balance_usd) over (partition by address order by date rows between 90 preceding and current row) as balance_usd_90d_avg
            , avg(balance_usd) over (partition by address order by date rows between 180 preceding and current row) as balance_usd_180d_avg
            , native_token_balance
            , avg(native_token_balance) over (partition by address order by date rows between 30 preceding and current row) as native_token_balance_30d_avg
            , avg(native_token_balance) over (partition by address order by date rows between 90 preceding and current row) as native_token_balance_90d_avg 
            , avg(native_token_balance) over (partition by address order by date rows between 180 preceding and current row) native_token_balance_180d_avg
            , stablecoin_balance
            , avg(stablecoin_balance) over (partition by address order by date rows between 30 preceding and current row) as stablecoin_balance_30d_avg
            , avg(stablecoin_balance) over (partition by address order by date rows between 90 preceding and current row) as stablecoin_balance_90d_avg
            , avg(stablecoin_balance) over (partition by address order by date rows between 180 preceding and current row) as stablecoin_balance_180d_avg
        from last_values_carried_forward
    )

    select
        a.date
        , a.address 
        , '{{ chain }}' as chain
        , a.balance_usd 
        , a.balance_usd_30d_avg 
        , a.balance_usd_90d_avg
        , a.balance_usd_180d_avg
        , a.native_token_balance
        , a.native_token_balance_30d_avg 
        , a.native_token_balance_90d_avg 
        , a.native_token_balance_180d_avg
        , a.stablecoin_balance 
        , a.stablecoin_balance_30d_avg 
        , a.stablecoin_balance_90d_avg 
        , a.stablecoin_balance_180d_avg
    from addresses_with_balance_rolling_avgs a
    join balances b -- join balances back in to limit table to rows in fact_arbitrum_daily_balances tables; will just include averages now as well; otherwise table will be massive depending on chain if we include full cross joined table
        on a.date = b.date
        and a.address = b.address
{% endmacro %}
