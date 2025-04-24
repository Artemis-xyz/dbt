{{ config(materialized="view") }}
select
    date_trunc('day', block_timestamp) as date,
    count(distinct authorizers) as daa,
    count(distinct tx_id) as txns,
    'flow' as chain
from flow_flipside.core.fact_transactions
where tx_succeeded = TRUE
group by date

