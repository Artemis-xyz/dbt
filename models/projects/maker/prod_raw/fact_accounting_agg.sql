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
            acc.dai_value as usd_value,
            acc.value as token_value,
            token
        from {{ ref('fact_final') }} acc
    ),
    accounting_agg as (
        select
            date_trunc('day', period) as period,
            account_id,
            token,
            sum(coalesce(token_value,0)) as token_sum_value,
            sum(coalesce(usd_value, 0)) AS usd_sum_value
        from accounting
        group by 1,2,3
    ),
    accounting_liq as (
        select distinct
            period,
            token,
            sum(coalesce(token_sum_value, 0)) over (partition by date_trunc('day', period)) as token_liq_cum,
            sum(coalesce(usd_sum_value, 0)) over (partition by date_trunc('day', period)) as usd_liq_cum
        from accounting_agg
        where account_id in (
            '31210', -- Liquidation Revenues
            '31620'  -- Liquidation Expenses
        )
    )
select
    a.period,
    a.account_id,
    a.token,
    case
        when account_id = '31210' then iff(usd_liq_cum > 0, usd_liq_cum, 0)
        when account_id = '31620' then iff(usd_liq_cum > 0, 0, usd_liq_cum)
        else coalesce(usd_sum_value, 0)
    end as sum_value,
    case
        when account_id = '31210' then iff(token_liq_cum > 0, token_liq_cum, 0)
        when account_id = '31620' then iff(token_liq_cum > 0, 0, token_liq_cum)
        else coalesce(token_sum_value,0)
    end as sum_value_token
from accounting_agg a
left join accounting_liq l
    on a.period = l.period
    and a.token = l.token