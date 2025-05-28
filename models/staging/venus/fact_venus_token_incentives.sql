{{ config(materialized="table", snowflake_warehouse="VENUS") }}


with venus_token_incentives as (
    select
        date(block_timestamp) as date,
        'BSC' as chain,
        sum(
            case
                when amount_usd is not null then amount_usd
                when lower(contract_address) = lower('0xcf6bb5389c92bdda8a3747ddb454cb7a64626c63') then (raw_amount/1e18)*eph.price
                else 0 end
        ) as token_incentives
    from bsc_flipside.core.ez_token_transfers as tt
    join bsc_flipside.price.ez_prices_hourly as eph
        on lower(eph.symbol) = lower('XVS') and eph.hour = date_trunc('hour', tt.block_timestamp)
    where lower(from_address) = lower('0xfD36E2c2a6789Db23113685031d7F16329158384')
    AND tx_hash NOT IN (
                '0xf5f7bf1544b9262c155636455798c6ad01f1ec4504525997fbee7e6e247e33fa',
                '0xe0bd2a910b50942e6292a5072454704655d725007167f5c148ac9731417e5a7e',
                '0xf43966308d84af1ec3d58da914435282bb6272d5b4141349bf22588eb2054d25',
                '0x6c4fe697bea7278c871d526395fa13e1d65dcde49d9008ee628f55f4250fd523'
                )
    group by date
)

select * from venus_token_incentives
