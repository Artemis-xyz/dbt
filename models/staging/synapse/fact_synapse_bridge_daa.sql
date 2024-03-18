with
    daily_addresses as (
        select
            date_trunc('day', destination_block_timestamp) as date,
            count(distinct user) as bridge_daa
        from
            (
                select destination_block_timestamp, depositor as user
                from {{ ref("fact_synapse_transfers") }}

                union

                select destination_block_timestamp, recipient as user
                from {{ ref("fact_synapse_transfers") }}
            ) t
        group by 1
        order by 1 asc
    )

select date, bridge_daa, 'synapse' as app, null as chain, 'Bridge' as category
from daily_addresses
