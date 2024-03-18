with
    daily_addresses as (
        select date_trunc('day', timestamp) as date, count(distinct user) as bridge_daa
        from
            (
                select timestamp, from_address as user
                from {{ ref("fact_wormhole_transfers") }}

                union

                select timestamp, to_address as user
                from {{ ref("fact_wormhole_transfers") }}
            ) t
        group by 1
        order by 1 asc
    )

select date, bridge_daa, 'wormhole' as app, null as chain, 'Bridge' as category
from daily_addresses
