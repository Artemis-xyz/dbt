{% macro flipside_lending_flashloan_fees(chain, protocol) %}
select 
    block_timestamp::date as date
    , '{{chain}}' as chain
    , '{{protocol}}' as protocol
    , flashloan_token as token_address
    , sum(premium_amount) as amount_nominal
    , sum(coalesce(premium_amount_usd, 0)) as amount_usd
from {{chain}}_flipside.defi.ez_lending_flashloans 
where platform = '{{protocol}}'
group by 1, 2, 3, 4
{% endmacro %}