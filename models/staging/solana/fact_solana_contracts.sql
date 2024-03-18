{{ config(materialized="incremental", unique_key="date") }}

with
    deployments as (
        select block_timestamp, tx_id
        from solana_flipside.core.fact_events
        where
            program_id = 'BPFLoaderUpgradeab1e11111111111111111111111'
            and solana_flipside.core.fact_events.succeeded = true
            and event_type = 'deployWithMaxDataLen'
            and solana_flipside.core.fact_events.block_timestamp is not null
            {% if is_incremental() %}
                and block_timestamp >= (select max(date) from {{ this }})
            {% endif %}
    ),
    transactions as (
        select *
        from solana_flipside.core.fact_transactions
        {% if is_incremental() %}
            where block_timestamp >= (select max(date) from {{ this }})
        {% endif %}
    )

select
    date_trunc('week', deployments.block_timestamp) as date,
    count(*) contracts_deployed,
    count(distinct(signers[0])) contract_deployers,
    'solana' as chain
from deployments
join transactions on deployments.tx_id = transactions.tx_id
where deployments.block_timestamp is not null
group by 1
order by 1 desc
