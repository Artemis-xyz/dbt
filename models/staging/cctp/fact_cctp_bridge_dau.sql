with
    daily_addresses as (
        select block_timestamp::date as date, count(distinct user) as bridge_dau
        from
            (
                select block_timestamp, sender as user
                from {{ ref("fact_cctp_transfers") }}

                union

                select block_timestamp, reciepient as user
                from {{ ref("fact_cctp_transfers") }}
            ) t
        group by 1
        order by 1 asc
    )

select date, bridge_dau, 'cctp' as app, null as chain, 'Bridge' as category
from daily_addresses
