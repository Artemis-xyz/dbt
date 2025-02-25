{{
    config(
        materialized="table",
        snowflake_warehouse="FREQUENCY",
    )
}}

with
parity_transactions as (
    select
        date
        , count(*) as txns
        , count(distinct signer_id) as daa
        , sum(fees_native) as fees_native
    from {{ ref("fact_frequency_transactions") }} as t
    where signer_id is not null
    group by date
)
select
    date
    , coalesce(txns,0) as txns
    , coalesce(daa,0) as daa
    , coalesce(fees_native,0) as fees_native
from parity_transactions 
where date < to_date(sysdate())
