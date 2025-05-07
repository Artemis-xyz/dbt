{% macro get_bananagun_fees(chain) %}
    WITH token_prices AS (
        SELECT
            DATE_TRUNC('hour', HOUR) as price_hour,
            CASE WHEN is_native THEN 'So11111111111111111111111111111111111111111'
            ELSE TOKEN_ADDRESS END as token_address,
            PRICE as token_price
        FROM
            {% if chain == 'solana' %}
                SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY
                WHERE 1=1
                AND BLOCKCHAIN = 'solana'
            {% else %}
                ETHEREUM_FLIPSIDE.PRICE.EZ_PRICES_HOURLY
                WHERE SYMBOL = 'ETH'
                AND BLOCKCHAIN = 'ethereum'
                AND IS_NATIVE = TRUE
            {% endif %}
    ),
    all_fees AS (
        {% if chain == 'solana' %}
            SELECT
                'SOLANA' as source,
                TX_ID as transaction_hash,
                "INDEX" as index,
                AMOUNT as fee_amount,
                BLOCK_TIMESTAMP,
                fee_amount * tp.token_price as fee_usd
            FROM
                SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS ft
                LEFT JOIN token_prices tp 
                    ON DATE_TRUNC('hour', ft.BLOCK_TIMESTAMP) = tp.price_hour
                    AND ft.mint = tp.token_address
            WHERE
                TX_TO IN (
                    '47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF',
                    '4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7',
                    '8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2'
                )
                {% if is_incremental() %}
                    AND BLOCK_TIMESTAMP > (SELECT dateadd('day', -3, MAX(BLOCK_TIMESTAMP)) FROM {{ this }})
                {% else %}
                    AND BLOCK_TIMESTAMP > '2023-12-30'
                {% endif %}
        {% else %}
            SELECT
                upper('{{ chain }}') as source,
                TX_HASH as transaction_hash,
                EVENT_INDEX as index,
                (PC_DBT_DB.PROD.HEX_TO_INT(substr(DATA, 3, 64))) / pow(10, 18) as fee_amount,
                BLOCK_TIMESTAMP,
                fee_amount * tp.token_price as fee_usd
            FROM
                {{chain}}_FLIPSIDE.CORE.FACT_EVENT_LOGS e
                JOIN {{ ref('dim_bananagun_' ~ chain ~ '_contracts') }} c 
                    ON lower(e.ORIGIN_TO_ADDRESS) = lower(c.contract_address)
                LEFT JOIN token_prices tp 
                    ON DATE_TRUNC('hour', e.BLOCK_TIMESTAMP) = tp.price_hour
            WHERE
                TOPICS[0] IN ('0x72015ace03712f361249380657b3d40777dd8f8a686664cab48afd9dbbe4499f','0x0c2a2f565c7774c59e49ef6b3c255329f4d254147e06e724d3a8569bb7bd21ad')
                {% if is_incremental() %}
                    AND BLOCK_TIMESTAMP > (SELECT dateadd('day', -3, MAX(BLOCK_TIMESTAMP)) FROM {{ this }})
                {% endif %}
        {% endif %}
    )
    SELECT 
        source,
        transaction_hash,
        index,
        fee_amount,
        BLOCK_TIMESTAMP,
        fee_usd
    FROM all_fees
{% endmacro %}
