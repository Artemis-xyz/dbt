{{ config(materialized="table", snowflake_warehouse="GMX") }}

with spot_transactions as (
    select
        s.date,
        count(distinct s.trader) as spot_dau,
        count(distinct s.tx_hash) as spot_txns
    from {{ref('fact_gmx_all_versions_dex_swaps')}} s
    group by s.date
), perp_transactions as (
    select
        p.date,
        count(distinct p.trader) as perp_dau,
        count(distinct p.tx_hash) as perp_txns
    from {{ref('fact_gmx_all_versions_perp_trades')}} p
    group by p.date
), all_transactions as (
    select
        date,
        trader,
        tx_hash
    from {{ref('fact_gmx_all_versions_dex_swaps')}}
    union all
    select
        date,
        trader,
        tx_hash
    from {{ref('fact_gmx_all_versions_perp_trades')}}
), all_transactions_dau as (
    select
        a.date,
        count(distinct a.trader) as gmx_dau,
        count(distinct a.tx_hash) as gmx_txns
    from all_transactions a
    group by a.date
)
select
    *
from spot_transactions s
left join perp_transactions p using(date)
left join all_transactions_dau a using(date)
