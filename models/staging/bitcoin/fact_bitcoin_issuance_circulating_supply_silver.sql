{{
    config(
        materialized="table",
    )
}}
select
    to_date(block_timestamp) as date,
    'bitcoin' as chain,
    sum(
        50 / pow(2, floor(block_number / 210000)) + case
            when block_number = 1
            then 50  -- initial supply at block 0
            else 0
        end
    ) as issuance,
    sum(issuance) over (order by date) as circulating_supply
from bitcoin_flipside.core.fact_blocks
where date < to_date(sysdate())
group by date
