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

    kamino_fees as (
        select date, klend_fees_usd as fees
        from {{ ref("fact_kamino_fees_and_revenues") }}
    ),

    kamino_revenue as (
        select date, klend_revenue_usd as revenue
        from {{ ref("fact_kamino_fees_and_revenues") }}
    )

    select
        date,
        tvl,
        fees,
        revenue
    from kamino_tvl
    left join kamino_fees using (date)
    left join kamino_revenue using (date)
    where date < to_date(sysdate())
