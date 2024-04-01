{{ config(materialized="table") }}
select date, contract_deployers, contracts_deployed, chain, source
from {{ ref("fact_fantom_contracts") }}
