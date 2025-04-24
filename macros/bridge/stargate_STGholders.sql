{% macro stargate_stg_holders(chain, token_contract_address, stake_contract_address) %} 
with
stg_balances as (
    select 
        address
        , balance_token / 1e18 as stg_balance
        , block_timestamp
        , row_number() over (
            partition by address 
            order by block_timestamp desc
        ) as today
    from {{ ref("fact_"~ chain ~"_address_balances_by_token")}}
    where lower(contract_address) = lower('{{token_contract_address}}')
),
circulating_supply as (
    select
        date
        , treasury_balance
        , vesting_balance
        , circulating_supply
        , treasury_balance + vesting_balance + circulating_supply as total_supply
    from {{ ref("fact_stargate_circulating_supply")}}
    order by date desc
    limit 1
),
top_holders as (
    select 
        e.address
        , e.stg_balance
        , c.circulating_supply
        , case 
            -- Vesting wallet that is still doing linear unlocks
            when lower(e.address) = lower('0x8A27E7e98f62295018611DD681Ec47C7d9FF633A') 
            then 'Locked' 
            else 'Unlocked' 
        end as status
        , (e.stg_balance / c.total_supply) * 100 AS percentage_of_total_supply
    from stg_balances e
    cross join circulating_supply c
    where e.today = 1
    order by e.stg_balance desc
    limit 100
),
{% if chain == "avalanche" %}
net_staked_withdrawal AS (
    select 
        lower(fded.decoded_log:from::STRING) as staker_address
        , sum(fded.decoded_log:value::NUMERIC / 1e18) as value
    from {{chain}}_flipside.core.ez_decoded_event_logs fded
    where 
        lower(fded.contract_address) = lower('{{token_contract_address}}')
        and lower(fded.decoded_log:to::STRING) = lower('{{stake_contract_address}}')
    group by lower(fded.decoded_log:from::STRING)

    UNION ALL

    select 
        lower(fded.decoded_log:to::STRING) as staker_address
        , sum(fded.decoded_log:value::NUMERIC / 1e18) * -1 as value
    from {{chain}}_flipside.core.ez_decoded_event_logs fded
    where 
        lower(fded.contract_address) = lower('{{token_contract_address}}')
        and lower(fded.decoded_log:from::STRING) = lower('{{stake_contract_address}}')
    group by lower(fded.decoded_log:to::STRING)
),
{% else %}
net_staked_withdrawal AS (
    select 
        lower(fded.decoded_log:provider::STRING) as staker_address
        , sum(fded.decoded_log:value::NUMERIC / 1e18) as value
    from {{chain}}_flipside.core.ez_decoded_event_logs fded
    where 
        lower(fded.contract_address) = lower('{{stake_contract_address}}') 
        and fded.event_name = 'Deposit'
    group by lower(fded.decoded_log:provider::STRING)

    UNION ALL

    select 
        lower(fdew.decoded_log:provider::STRING) as staker_address
        , sum(fdew.decoded_log:value::NUMERIC / 1e18) * -1 as value
    from {{chain}}_flipside.core.ez_decoded_event_logs fdew
    where 
        lower(fdew.contract_address) = lower('{{stake_contract_address}}')
        and fdew.event_name = 'Withdraw'
    group by lower(fdew.decoded_log:provider::STRING)
),
{% endif %}
net_balances AS (
    select
        staker_address
        , case 
            when sum(value) < 0 then 0 
            else sum(value) 
        end as net_balance
    from net_staked_withdrawal
    group by staker_address
    order by net_balance desc
),
holder_stake_status AS (
    select 
        th.address
        , th.stg_balance
        , th.status
        , th.percentage_of_total_supply
        , coalesce(nb.net_balance, 0) as staked_balance
        , case 
            WHEN nb.net_balance IS NOT NULL THEN TRUE 
            ELSE FALSE 
        end as stake_status,
        coalesce(nb.net_balance, 0) / nullif(th.stg_balance, 0) * 100 as stake_percentage
    from top_holders th
    left join net_balances nb on lower(th.address) = lower(nb.staker_address)
)
select 
    address
    , stg_balance
    , status
    , percentage_of_total_supply
    , staked_balance
    , stake_status
    , stake_percentage
    , '{{chain}}' as chain
from holder_stake_status
order by stg_balance desc

{% endmacro %}