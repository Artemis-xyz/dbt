with
    daily_addresses as (
        select date_trunc('day', src_timestamp) as date, count(distinct user) as bridge_daa
        from
            (
                select src_timestamp, from_address as user
                from {{ ref("fact_wormhole_operations_with_price") }}

                union

                select src_timestamp, to_address as user
                from {{ ref("fact_wormhole_operations_with_price") }}
            ) t
        group by 1
        order by 1 asc
    )

select date, bridge_daa, 'wormhole' as app, null as chain, 'Bridge' as category
from daily_addresses
