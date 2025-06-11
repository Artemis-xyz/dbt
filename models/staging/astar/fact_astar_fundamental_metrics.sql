{{
    config(
        materialized="table",
        snowflake_warehouse="ASTAR",
    )
}}

with evm_transactions as (
    select
        date
        , count(distinct sender) as daa
        , count(*) as txns
        , sum(fees) as fees
        , sum(fees_native) as fees_native
    from {{ ref("fact_astar_evm_transactions") }}
    group by date
),
parity_transactions as (
    select
        date
        , count(*) as txns
        , count(distinct signer_id) as daa
        , sum(fees_native) as fees_native
        , sum(fees_usd) as fees_usd
    from {{ ref("fact_astar_transactions") }} as t
    where signer_id is not null
    group by date
)
select
    coalesce(t.date, e.date) as date
    , coalesce(t.txns,0) + coalesce(e.txns,0) as txns
    , coalesce(t.daa,0) + coalesce(e.daa, 0) as daa
    , coalesce(t.fees_native,0) + coalesce(e.fees_native, 0) as fees_native
    , coalesce(t.fees_usd,0) + coalesce(e.fees, 0) as fees_usd
from parity_transactions as t
full join evm_transactions as e on t.date = e.date
    and coalesce(t.date, e.date) < to_date(sysdate())
