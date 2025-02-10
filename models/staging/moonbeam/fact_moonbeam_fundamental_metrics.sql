{{
    config(
        materialized="table",
        snowflake_warehouse="MOONBEAM",
    )
}}


with evm_transactions as (
    select
        date(timestamp) as date
        , count(distinct receiver) as daa
        , count(*) as txns
    from {{ ref("fact_moonbeam_evm_transactions") }}
    group by date
),
parity_transactions as (
    select
        date
        , count(*) as txns
        , count(distinct signer_id) as daa
        , sum(fees_native) as fees_native
        , sum(fees_usd) as fees_usd
    from {{ ref("fact_moonbeam_transactions") }} as t
    where signer_id is not null
    group by date
)
select
    coalesce(t.date, e.date) as date
    , t.txns + e.txns as txns
    , t.daa + e.daa as daa
    , t.fees_native as fees_native
    , t.fees_usd as fees_usd
from parity_transactions as t
left join evm_transactions as e on t.date = e.date
    and coalesce(t.date, e.date) < to_date(sysdate())
