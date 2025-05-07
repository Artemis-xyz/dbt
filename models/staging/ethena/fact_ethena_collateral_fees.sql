select
    date(block_timestamp) as date,
    sum(
        TRY_TO_NUMBER(NULLIF(decoded_log:jlpplus_amount :: string, '')) / 1e18
    ) as collateral_fee
from {{ref('fact_ethereum_decoded_events')}}
where
    lower(contract_address) = lower('0x2cc440b721d2cafd6d64908d6d8c4acc57f8afc3')
    and event_name = 'Mint'
group by date(block_timestamp)
order by date(block_timestamp) desc