{% macro get_pendle_deposit_redeem_txns(chain) %}

    WITH sy_addresses AS (
        SELECT sy_address
        FROM {{ref('dim_pendle_' ~ chain ~ '_market_metadata')}}
    )
    , deposits AS (
        SELECT 
            block_timestamp,
            tx_hash,
            event_index,
            contract_address,
            'Deposit' as event_type,
            -- Indexed parameters from topics
            '0x' || RIGHT(topics[1], 40) as caller_address,
            '0x' || RIGHT(topics[2], 40) as receiver_address,
            '0x' || RIGHT(topics[3], 40) as token_address,  -- tokenIn for deposits
            -- Unindexed parameters from data
            PC_DBT_DB.PROD.HEX_TO_INT(SUBSTRING(data, 3, 64)) as amount_in,   -- amountDeposited
            PC_DBT_DB.PROD.HEX_TO_INT(SUBSTRING(data, 67, 64)) as amount_out  -- amountSyOut
        FROM {{ chain }}_flipside.core.fact_event_logs
        WHERE topics[0] in ('0x5fe47ed6d4225326d3303476197d782ded5a4e9c14f479dc9ec4992af4e85d59') -- Deposit event
        AND contract_address in (SELECT sy_address FROM sy_addresses)
        {% if is_incremental() %}
            AND block_timestamp > (SELECT DATEADD(day, -1, MAX(block_timestamp)) FROM {{this}})
        {% endif %}
    ),

    redeems AS (
        SELECT 
            block_timestamp,
            tx_hash,
            event_index,
            contract_address,
            'Redeem' as event_type,
            -- Indexed parameters from topics
            '0x' || RIGHT(topics[1], 40) as caller_address,
            '0x' || RIGHT(topics[2], 40) as receiver_address,
            '0x' || RIGHT(topics[3], 40) as token_address,  -- tokenOut for redeems
            -- Unindexed parameters from data
            PC_DBT_DB.PROD.HEX_TO_INT(SUBSTRING(data, 3, 64)) as amount_in,   -- amountSyToRedeem
            PC_DBT_DB.PROD.HEX_TO_INT(SUBSTRING(data, 67, 64)) as amount_out  -- amountTokenOut
        FROM {{ chain }}_flipside.core.fact_event_logs
        WHERE topics[0] = '0xaee47cdf925cf525fdae94f9777ee5a06cac37e1c41220d0a8a89ed154f62d1c' -- Redeem event
        AND contract_address in (SELECT sy_address FROM sy_addresses)
        {% if is_incremental() %}
            AND block_timestamp > (SELECT DATEADD(day, -1, MAX(block_timestamp)) FROM {{this}})
        {% endif %}
    )
    SELECT
        block_timestamp,
        tx_hash,
        event_index,
        contract_address as sy_address,
        amount_in::number as amount
    FROM deposits
    UNION ALL
    SELECT
        DECODED_LOG:tokenOut::STRING as token_address,
        - DECODED_LOG:amountTokenOut::number as amount,
        contract_address as sy_address,
        amount_out::number as amount,
        tx_hash,
        event_index
    FROM redeems:wq

{% endmacro %}