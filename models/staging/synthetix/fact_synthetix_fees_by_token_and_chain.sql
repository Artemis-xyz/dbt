{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

with optimism_fees as (
    select
        date(tt.block_timestamp) as date, 
        tt.symbol as token, 
        'Optimism' as chain, 
        sum(coalesce(tt.amount, 0) * eph.price) as fees_usd, 
        sum(coalesce(tt.amount, 0)) as fees_native
    from optimism_flipside.core.ez_token_transfers as tt
    join ethereum_flipside.price.ez_prices_hourly as eph
        on hour = date_trunc('hour', tt.block_timestamp) and lower(tt.symbol) = lower(eph.symbol)
    where lower(to_address) = lower('0xfeefeefeefeefeefeefeefeefeefeefeefeefeef')
    group by 1, 2, 3
    order by 1, 2, 3
), 

ethereum_fees as (
    with adjusted_transfers as (
        select
            block_timestamp,
            tx_hash, 
            contract_address, 
            case 
                when lower(contract_address) in (
                    lower('0x57ab1ec28d129707052df4df418d58a2d46d5f51'), 
                    lower('0x57ab1e02fee23774580c119740129eac7081e9d3')
                ) then 'sUSD'
                when lower(contract_address) = lower('0x0a51952e61a990e585316caa3d6d15c8d3e55976') then 'pUSD'
                else null
            end as token,
            raw_amount/1e18 as amount_adj,
            to_address
        from ethereum_flipside.core.ez_token_transfers
        where lower(to_address) = lower('0xfeefeefeefeefeefeefeefeefeefeefeefeefeef') and block_timestamp > '2020-01-01' --one transaction in 2018 for negligble amount, so just ignored that
    )

    select
        date(at.block_timestamp) as date,
        at.token,
        'Ethereum' as chain,
        sum(coalesce(at.amount_adj, 0) * eph.price) as fees_usd,
        sum(coalesce(at.amount_adj, 0)) as fees_native
    from adjusted_transfers at
    join ethereum_flipside.price.ez_prices_hourly eph
        on eph.hour = date_trunc('hour', at.block_timestamp)
        and lower(eph.symbol) = lower(at.token)
    where at.token is not null
    group by 1, 2, 3
    order by 1, 2, 3
)

select * from optimism_fees
union all
select * from ethereum_fees