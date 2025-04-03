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
        select date, sum(usd_balance) as tvl
        from {{ ref("fact_kamino_tvl") }}
        group by date
    ),

    klend_fees_and_revenue as (
        select date, klend_fees_usd as fees, klend_revenue_usd as revenue
        from {{ ref("fact_kamino_fees_and_revenues") }}
    ),

    kamino_transactions as (    
        select date, tx_count as txn, dau
        from {{ ref("dim_kamino_transactions") }}
    ),

    market_data as (
        {{ get_coingecko_metrics('kamino') }}
    )
    select
        date,
        tvl,
        fees,
        revenue,
        txn as txns,
        dau,
        coalesce(market_data.price, 0) as price,
        coalesce(market_data.market_cap, 0) as market_cap,
        coalesce(market_data.fdmc, 0) as fdmc,
        coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating,
        coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv,
        coalesce(market_data.token_volume, 0) as token_volume,
    from kamino_tvl
    left join klend_fees_and_revenue using (date)
    left join market_data using (date)
    left join kamino_transactions using (date)
    where date < to_date(sysdate())
