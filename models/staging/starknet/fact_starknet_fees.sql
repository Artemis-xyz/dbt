with fees_data as (
    select
    date_trunc('day', raw_date) as date
    , currency
    , tx_fee
    from {{ ref("fact_starknet_transactions") }}
)
select
    date
    , sum(tx_fee) as fees_native
from fees_data
where currency = 'FRI'
group by 1
