{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

select
    date(block_timestamp) as date,
    'Ethereum' as chain,
    sum(
        case
            when amount_usd is not null then amount_usd
            when contract_address ILIKE '0xc011a72400e58ecd99ee497cf89e3775d4bd732f' then (raw_amount/1e18)*eph.price
            else 0 end
    ) as token_incentives
from ethereum_flipside.core.ez_token_transfers as tt
join ethereum_flipside.price.ez_prices_hourly as eph
    on eph.symbol ILIKE 'SNX' and eph.hour = date_trunc('hour', tt.block_timestamp)
where lower(from_address) in (lower('0x29C295B046a73Cde593f21f63091B072d407e3F2'), lower('0xFfA72Fd80d8A84032d855bfb67036BAF45949009'))
group by date
