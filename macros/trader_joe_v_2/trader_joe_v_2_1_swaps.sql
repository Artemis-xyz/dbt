{% macro get_trader_joe_v_2_1_swaps_for_chain(factory_address, chain, version) %}

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
,swaps AS (              -- Step 2: Use the extracted LBPair addresses to filter Swap events
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        CONTRACT_ADDRESS as lbpair_address,
        PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(DECODED_LOG:"amountsIn" :: string, 3, 32)) AS amount_Y,  -- first 128 bits is Y
        PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(DECODED_LOG:"amountsIn" :: string, 35, 32)) AS amount_X,  -- last 128 bits from the right is X

        PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(DECODED_LOG:"totalFees":: string, 3, 32))::number AS fees_Y,
        PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(DECODED_LOG:"totalFees":: string, 35, 32))::number AS fees_X,
        fees_Y + fees_X as native_fees,

        PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(DECODED_LOG:"protocolFees":: string, 3, 32))::number AS protocol_fees_Y,
        PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(DECODED_LOG:"protocolFees":: string, 35, 32))::number AS protocol_fees_X,
        protocol_fees_Y + protocol_fees_X as protocol_fees
    FROM
        {{chain}}_flipside.core.ez_decoded_event_logs
    WHERE
        LOWER(contract_address) IN (
            SELECT lbpair_address FROM lbpairs
        )
    AND event_name = 'Swap' 
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
    (s.protocol_fees * p.price) / pow(10, p.decimals) as protocol_fees_usd,
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