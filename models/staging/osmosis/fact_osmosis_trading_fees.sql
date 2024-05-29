with
    raw as (
        select 
            block_date::date as date, 
            sum(coalesce(fees_usd, 0)) as trading_fees
        from osmosis_flipside.defi.fact_pool_fee_day 
        where currency not in (
            'ibc/A23E590BA7E0D808706FB5085A449B3B9D6864AE4DDE7DAF936243CEBB2A3D43',
            'ibc/5F5B7DA5ECC80F6C7A8702D525BB0B74279B1F7B8EFAE36E423D68788F7F39FF',
            'factory/osmo1z0qrq605sjgcqpylfl4aa6s90x738j7m58wyatt0tdzflg2ha26q67k743/wbtc',
            'factory/osmo1q77cw0mmlluxu0wr29fcdd0tdnh78gzhkvhe4n6ulal9qvrtu43qtd0nh8/wiha',
            'factory/osmo19hdqma2mj0vnmgcxag6ytswjnr8a3y07q7e70p/wLIBRA'
        )
        group by date
    )
select raw.date, 'osmosis' as chain, trading_fees
from raw
where date < to_date(sysdate())
