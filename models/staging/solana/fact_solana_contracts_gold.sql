{{ config(materialized="table") }}
select date, contracts_deployed, contract_deployers, chain
from {{ ref("fact_solana_contracts") }}
where date < to_date(sysdate())
