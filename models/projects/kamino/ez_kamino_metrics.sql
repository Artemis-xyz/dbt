{{
    config(
        materialized="table",
        snowflake_warehouse="KAMINO",
        database="kamino",
        schema="core",
        alias="ez_metrics",
    )
}}

with 
    kamino_tvl as (
        select date, total_value_locked as tvl
        from {{ ref("fact_kamino_total_value_locked") }}
    ),

    klend_fees as (
        select date, klend_fees_usd as fees
        from {{ ref("fact_kamino_fees_and_revenues") }}
    ),

    klend_revenue as (
        select date, klend_revenue_usd as revenue
        from {{ ref("fact_kamino_fees_and_revenues") }}
    ), 

    kamino_transactions as (    
        select date, tx_count as daily_transactions
        from {{ ref("fact_kamino_daily_transactions") }}
    ),

    kamino_active_users as (
        select date, distinct_signers_count as daily_active_users
        from {{ ref("fact_kamino_daily_active_users") }}
    ),

    market_data as (
        {{ get_coingecko_metrics('kamino') }}
    )
    select
        date,
        tvl,
        fees,
        revenue,
        daily_transactions,
        daily_active_users,
        coalesce(market_data.price, 0) as price,
        coalesce(market_data.market_cap, 0) as market_cap,
        coalesce(market_data.fdmc, 0) as fdmc,
        coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating,
        coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv,
        coalesce(market_data.token_volume, 0) as token_volume,
    from kamino_tvl
    left join klend_fees using (date)
    left join klend_revenue using (date)
    left join market_data using (date)
    left join kamino_transactions using (date)
    left join kamino_active_users using (date)
    where date < to_date(sysdate())
