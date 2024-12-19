{{config(materialized='table')}}

select instruction:parsed:info:account::string as address, count(*) as contract_deployments
from solana_flipside.core.fact_events 
where program_id = 'BPFLoaderUpgradeab1e11111111111111111111111' 
group by 1