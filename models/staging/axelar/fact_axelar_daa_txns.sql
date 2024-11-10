{{ config(materialized="table") }}
select
    block_timestamp::date as date,
    count(distinct tx_from) as daa,
    count(*) as txns,
    'axelar' as chain
from axelar_flipside.core.fact_transactions
where block_timestamp is not null
group by date
