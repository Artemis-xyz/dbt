{{ config(materialized="table") }}
select date, contract_deployers, contracts_deployed, chain
from {{ ref("fact_arbitrum_contracts") }}
