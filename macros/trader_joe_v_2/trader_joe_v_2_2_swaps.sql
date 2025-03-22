{% macro get_trader_joe_v_2_2_swaps_for_chain(factory_address, chain, version) %}
WITH lbpairs AS (        -- Step 1: Extract all Pools addresses that were created in the factory contract ``
    SELECT
    DECODED_LOG:"LBPair" :: string AS lbpair_address,
    DECODED_LOG:"tokenX" :: string AS token_X_address,
    DECODED_LOG:"tokenY" :: string AS token_Y_address
FROM
    {{chain}}_flipside.core.ez_decoded_event_logs
WHERE
    LOWER(contract_address) = LOWER('{{factory_address}}') -- factory contract 
    AND event_name = 'LBPairCreated' -- get all pools made in this version
)
, swaps AS (              -- Step 2: Use the extracted LBPair addresses to filter Swap events
    SELECT
        block_timestamp
        , tx_hash
        , origin_from_address
        , contract_address as lbpair_address
        , substr(data, 3, 64) as id
        , substr(data, 67, 64) as amountIn
        , PC_DBT_DB.PROD.HEX_TO_INT(substr(amountIn, 1, 32)) as amount_Y
        , PC_DBT_DB.PROD.HEX_TO_INT(substr(amountIn, 33, 32)) as amount_X
        , substr(data, 259, 64) as totalFees
        , substr(data, 323, 64) as protocolFees
        , PC_DBT_DB.PROD.HEX_TO_INT(substr(totalFees, 1, 32)) as fees_Y
        , PC_DBT_DB.PROD.HEX_TO_INT(substr(totalFees, 33, 32)) as fees_X
        , fees_Y + fees_X as native_fees
        , PC_DBT_DB.PROD.HEX_TO_INT(substr(protocolFees, 1, 32)) as protocol_fees_Y
        , PC_DBT_DB.PROD.HEX_TO_INT(substr(protocolFees, 33, 32)) as protocol_fees_X
        , protocol_fees_Y + protocol_fees_X as protocol_fees 
    FROM
        {{chain}}_flipside.core.fact_event_logs
    WHERE
    LOWER(contract_address) IN (
        SELECT
                lbpair_address
        FROM
            lbpairs
    )
    AND topics[0] = '0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70'
    {% if is_incremental() %}
        AND block_timestamp > (SELECT MAX(block_timestamp) FROM {{this}})
    {% endif %}
)
SELECT
    s.block_timestamp,
    '{{chain}}' as chain,
    '{{version}}' as version,
    'trader_joe' as app,
    s.tx_hash,
    s.origin_from_address as user_address,
    s.lbpair_address,
    s.amount_Y,
    s.fees_Y,
    s.amount_X,
    s.fees_X,  
    l.token_X_address,
    l.token_Y_address,
    ((s.amount_Y + s.amount_X) * p.price) / pow(10,p.decimals) as volume_usd,
    (s.native_fees * p.price) / pow(10,p.decimals) as fee_usd,
    (s.protocol_fees *p.price) / pow(10, p.decimals) as protocol_fees_usd,
    p.symbol,
    p.price,
    CASE  
        WHEN s.amount_Y = '0' THEN token_X_address -- amountY is a string. When Y is 0, you swap X for Y -> fee paid in X
            ELSE token_Y_address
        END AS fee_token_address 
FROM
    swaps s
    left join lbpairs l ON s.lbpair_address = l.lbpair_address
    left join {{chain}}_flipside.price.ez_prices_hourly p ON date_trunc('hour', block_timestamp) = p.hour
    AND p.token_address = fee_token_address 

{% endmacro %}