{{
    config(
        materialized="table",
        snowflake_warehouse="MYTHOS",
    )
}}

select
    date, 
    count(*) as txns,
    count(distinct signer_id) as daa, 
    sum(fees_native) as fees_native, 
    sum(fees_usd) as fees_usd
from {{ ref("fact_mythos_transactions") }}
where signer_id is not null
    and date < to_date(sysdate())
GROUP BY date
