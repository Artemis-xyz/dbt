{{ config(materialized="view") }}
select
    date_trunc('week', near_flipside.core.fact_actions_events.block_timestamp) as date,
    count(*) contracts_deployed,
    count(distinct(tx_signer)) contract_deployers,
    'near' as chain
from near_flipside.core.fact_actions_events
join
    near_flipside.core.fact_transactions
    on near_flipside.core.fact_actions_events.tx_hash
    = near_flipside.core.fact_transactions.tx_hash
where action_name = 'DeployContract'
group by 1
