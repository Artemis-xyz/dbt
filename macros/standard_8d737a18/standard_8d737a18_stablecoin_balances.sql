{% macro standard_8d737a18_stablecoin_balances(chain) %}

{% set backfill_date = '' %}
select
    date as date_day
    , '{{chain}}' as chain_name
    , ca.chain_agnostic_id as chain_id
    , address
    , case 
        when substr(ca.chain_agnostic_id, 0, 7) = 'eip155:' then lower(ca.chain_agnostic_id || ':' || replace(replace(contract_address, '0x', ''), '0:', '')) 
        else ca.chain_agnostic_id || ':' || replace(replace(contract_address, '0x', ''), '0:', '') 
    end as asset_id
    , symbol as asset_symbol
    , contract_address
    , stablecoin_supply as balance
    , stablecoin_supply_native as balance_native
    , unique_id
from {{ ref( "fact_"~ chain ~ "_stablecoin_balances") }} st
left join {{ ref("chain_agnostic_ids") }} ca
    on '{{chain}}' = ca.chain
where date < to_date(sysdate())
{% if backfill_date != '' %}
    and date < '{{ backfill_date }}'
{% endif %}
{% if is_incremental() %} 
    and date >= (select dateadd('day', -3, max(date_day)) from {{ this }})
{% endif %}
{% endmacro %}
