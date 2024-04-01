with
    daily_addresses as (
        select
            date_trunc('day', block_timestamp) as date,
            count(distinct user) as bridge_daa
        from
            (
                select block_timestamp, depositor as user
                from {{ ref("fact_polygon_pos_bridge_transfers") }}

                union

                select block_timestamp, recipient as user
                from {{ ref("fact_polygon_pos_bridge_transfers") }}
            ) t
        group by 1
        order by 1 asc
    )

select
    date, bridge_daa, 'polygon_pos_bridge' as app, null as chain, 'Bridge' as category
from daily_addresses
