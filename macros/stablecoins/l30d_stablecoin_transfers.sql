{% macro l30d_stablecoin_transfers(chain) %}
select 
    date::date as date
    , '{{chain}}' as chain
    , t1.symbol::string as symbol
    , t1.contract_address::string as contract_address
    , inflow::float as inflow
    , transfer_volume::float as transfer_volume
    , from_address::string as from_address
    , to_address::string as to_address
    , coingecko_id
from {{ ref("fact_" ~ chain ~ "_stablecoin_transfers") }} t1
left join {{ ref( "fact_" ~ chain ~ "_stablecoin_contracts") }} t2 on lower(t1.contract_address) = lower(t2.contract_address)
where date >= (select dateadd('day', -31, to_date(sysdate())))
{% endmacro %}