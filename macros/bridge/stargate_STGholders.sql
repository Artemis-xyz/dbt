{% macro stargate_stg_holders(chain, token_contract_address, stake_contract_address) %} 
with
stg_balances AS (
    SELECT 
        ADDRESS,
        BALANCE_TOKEN / 1e18 AS STG_BALANCE,
        BLOCK_TIMESTAMP,
        ROW_NUMBER() OVER (
            PARTITION BY ADDRESS 
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS today
    FROM {{ ref("fact_"~ chain ~"_address_balances_by_token")}}
    WHERE lower(contract_address) = lower('{{token_contract_address}}')
),
circulating_supply AS (
    SELECT 
        1e9 - COALESCE(
            (SELECT MAX(STG_BALANCE) 
             FROM stg_balances 
             WHERE lower(ADDRESS) = lower('0x8A27E7e98f62295018611DD681Ec47C7d9FF633A')
            ), 0
        ) AS circulating_supply
),
top_holders AS (
    SELECT 
        e.ADDRESS, 
        e.STG_BALANCE,
        CASE 
            -- Vesting wallet that is still doing linear unlocks
            WHEN lower(e.ADDRESS) = lower('0x8A27E7e98f62295018611DD681Ec47C7d9FF633A') 
            THEN 'Locked' 
            ELSE 'Unlocked' 
        END AS status,  
        (e.STG_BALANCE / c.circulating_supply) * 100 AS percentage_of_circulating_supply
    FROM stg_balances e
    CROSS JOIN circulating_supply c
    WHERE e.today = 1
    ORDER BY e.STG_BALANCE DESC
    LIMIT 100
),
{% if chain == "avalanche" %}
net_staked_withdrawal AS (
    SELECT 
        LOWER(fded.decoded_log:from::STRING) AS staker_address,  
        SUM(fded.decoded_log:value::NUMERIC / 1e18) AS value
    FROM {{chain}}_flipside.core.fact_decoded_event_logs fded
    WHERE 
        LOWER(fded.contract_address) = lower('{{token_contract_address}}')
        AND LOWER(fded.decoded_log:to::STRING) = lower('{{stake_contract_address}}')
    GROUP BY LOWER(fded.decoded_log:from::STRING)

    UNION ALL

     SELECT 
        LOWER(fded.decoded_log:to::STRING) AS staker_address,  
        SUM(fded.decoded_log:value::NUMERIC / 1e18) * -1 AS value
    FROM {{chain}}_flipside.core.fact_decoded_event_logs fded
    WHERE 
        LOWER(fded.contract_address) = lower('{{token_contract_address}}')
        AND LOWER(fded.decoded_log:from::STRING) = lower('{{stake_contract_address}}')
    GROUP BY LOWER(fded.decoded_log:to::STRING)
),
{% else %}
net_staked_withdrawal AS (
    SELECT 
        LOWER(fded.decoded_log:provider::STRING) AS staker_address,  
        SUM(fded.decoded_log:value::NUMERIC / 1e18) AS value
    FROM {{chain}}_flipside.core.fact_decoded_event_logs fded
    WHERE 
        LOWER(fded.contract_address) = LOWER('{{stake_contract_address}}') 
        AND fded.event_name = 'Deposit'
    GROUP BY LOWER(fded.decoded_log:provider::STRING)

    UNION ALL

    SELECT 
        LOWER(fdew.decoded_log:provider::STRING) AS staker_address,  
        SUM(fdew.decoded_log:value::NUMERIC / 1e18) * -1 AS value
    FROM {{chain}}_flipside.core.fact_decoded_event_logs fdew
    WHERE 
        LOWER(fdew.contract_address) = LOWER('{{stake_contract_address}}')
        AND fdew.event_name = 'Withdraw'
    GROUP BY LOWER(fdew.decoded_log:provider::STRING)
),
{% endif %}
net_balances AS (
    SELECT
        staker_address,
        CASE 
            WHEN SUM(value) < 0 THEN 0 
            ELSE SUM(value) 
        END AS net_balance
    FROM
        net_staked_withdrawal
    GROUP BY
        staker_address
    ORDER BY
        net_balance DESC
),
holder_stake_status AS (
    SELECT 
        th.ADDRESS, 
        th.STG_BALANCE, 
        th.status, 
        th.percentage_of_circulating_supply,
        COALESCE(nb.net_balance, 0) AS staked_balance,
        CASE 
            WHEN nb.net_balance IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS stake_status,
        COALESCE(nb.net_balance, 0) / NULLIF(th.STG_BALANCE, 0) * 100 AS stake_percentage
    FROM top_holders th
    LEFT JOIN net_balances nb ON LOWER(th.ADDRESS) = LOWER(nb.staker_address)
)
SELECT 
    address,
    stg_balance,
    status,
    percentage_of_circulating_supply,
    staked_balance,
    stake_status,
    stake_percentage,
    '{{chain}}' as chain
FROM holder_stake_status
ORDER BY stg_balance DESC
{% endmacro %}