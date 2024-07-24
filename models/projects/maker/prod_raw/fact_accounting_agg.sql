{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_accounting_agg"
    )
}}



with
    chart_of_accounts as (
        select cast(code as varchar) as account_id from {{ ref('dim_chart_of_accounts') }} -- CoA simplified
    ),
    periods as (
        select
            distinct(date(hour)) as date
        from ethereum_flipside.price.ez_prices_hourly 
        where symbol = 'MKR'
    ),
    accounting as (
        select
            date_trunc('day', acc.ts) as period,
            cast(acc.code as varchar) as account_id,
            acc.dai_value as value
        from {{ ref('fact_final') }} acc
    ),
    accounting_agg as (
        select
            date_trunc('day', period) as period,
            account_id,
            sum(coalesce(value, 0)) AS sum_value
        from accounting
        group by 1,2
    ), -- GOOD UP TO HERE
    -- cumulative liquidation revenues & expenses. In next CTE (accounting_net): if positive, sum into revenues; if negative, sum into expenses
    accounting_liq as (
        select distinct
            period,
            sum(coalesce(sum_value, 0)) over (partition by date_trunc('day', period)) as liq_cum
        from accounting_agg
        where account_id in (
            '31210', -- Liquidation Revenues
            '31620'  -- Liquidation Expenses
        )
    ),
    accounting_net AS (
        select
            a.period,
            a.account_id,
            case
                when account_id = '31210' then iff(liq_cum > 0, liq_cum, 0)
                when account_id = '31620' then iff(liq_cum > 0, 0, liq_cum)
                else coalesce(sum_value, 0)
            end as sum_value,
            sum(case
                when account_id = '31210' then iff(liq_cum > 0, liq_cum, 0)
                when account_id = '31620' then iff(liq_cum > 0, 0, liq_cum)
                else coalesce(sum_value, 0)
            end) over (partition by a.account_id order by a.period) as cum_value
        from accounting_agg a
        left join accounting_liq l
            on a.period = l.period
    )

select * from accounting_net