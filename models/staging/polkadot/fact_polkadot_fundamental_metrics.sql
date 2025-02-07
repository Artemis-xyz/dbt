{{
    config(
        materialized="table",
        snowflake_warehouse="POLKADOT",
    )
}}

WITH
burns as (
    select
        max(chain) as chain,
        date,
        sum(burns_native) as burns_native,
        sum(burns) as burns
    from {{ ref("fact_polkadot_burned") }}
    where date < to_date(sysdate())
    group by date
),
fundamental_metrics as (
    select
        max(chain) as chain,
        date, 
        count(*) as txns,
        count(distinct signer_id) as dau, 
        sum(fees_native) as fees_native, 
        sum(fees_usd) as fees_usd
    from {{ ref("fact_polkadot_transactions") }} 
    where signer_id is not null
        and date < to_date(sysdate())
    GROUP BY date
)
SELECT 
    coalesce(f.chain, b.chain) as chain,
    coalesce(f.date, b.date) as date,
    coalesce(f.txns, 0) as txns,
    coalesce(f.dau, 0) as dau,
    coalesce(f.fees_native, 0) as fees_native,
    coalesce(f.fees_usd, 0) as fees_usd,
    coalesce(b.burns_native, 0) as burns_native,
    coalesce(b.burns, 0) as burns
FROM fundamental_metrics as f
LEFT JOIN burns as b on f.date = b.date
    and coalesce(f.date, b.date) < to_date(sysdate())
