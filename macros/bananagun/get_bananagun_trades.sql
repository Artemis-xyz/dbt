{% macro get_bananagun_trades(chain) %}

    {% if chain == 'solana' %}
        {% if is_incremental() %}
            WITH max_timestamp AS (
                SELECT dateadd('day', -3, MAX(BLOCK_TIMESTAMP)) as max_ts 
                FROM {{ this }}
            )
        {% endif %}
        
        SELECT
            'SOLANA' AS chain,
            s.BLOCK_TIMESTAMP,
            s.TX_ID as transaction_hash,
            s.SWAPPER as trader_address,
            CASE
                WHEN s.SWAP_FROM_SYMBOL = 'SOL' THEN s.SWAP_FROM_AMOUNT_USD
                WHEN s.SWAP_TO_SYMBOL = 'SOL' THEN s.SWAP_TO_AMOUNT_USD
                ELSE 0
            END as amount_in_usd,
            s.PROGRAM_ID as dex_platform,
            CASE
                WHEN s.SWAP_FROM_SYMBOL = 'SOL' THEN 'Buy'
                WHEN s.SWAP_TO_SYMBOL = 'SOL' THEN 'Sell'
                ELSE 'Other'
            END as trade_type
        FROM
            SOLANA_FLIPSIDE.DEFI.EZ_DEX_SWAPS s
        WHERE EXISTS (
            SELECT 1
            FROM SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS ft
            WHERE ft.TX_ID = s.TX_ID
            AND ft.TX_TO IN (
                '47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF',
                '4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7',
                '8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2'
            )
            {% if is_incremental() %}
                AND ft.BLOCK_TIMESTAMP > (SELECT max_ts FROM max_timestamp)
            {% endif %}
        )
        {% if is_incremental() %}
            AND s.BLOCK_TIMESTAMP > (SELECT max_ts FROM max_timestamp)
        {% endif %}

    {% else %}
        SELECT
            '{{ chain }}' AS chain,
            t.BLOCK_TIMESTAMP,
            t.TX_HASH as transaction_hash,
            t.ORIGIN_FROM_ADDRESS as trader_address,
            CASE
                WHEN LOWER(t.token_in) = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN t.AMOUNT_IN_USD
                ELSE t.AMOUNT_OUT_USD
            END as amount_in_usd,
            t.PLATFORM as dex_platform,
            CASE
                WHEN LOWER(t.token_in) = LOWER('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') THEN 'Buy'
                ELSE 'Sell'
            END as trade_type
        FROM
            {{ chain }}_FLIPSIDE.DEFI.EZ_DEX_SWAPS t
        WHERE EXISTS (
            SELECT 1
            FROM {{ ref('dim_bananagun_' ~ chain ~ '_contracts') }} c
            WHERE LOWER(t.ORIGIN_TO_ADDRESS) = LOWER(c.contract_address)
        )
        AND t.EVENT_INDEX = (
            SELECT MAX(EVENT_INDEX)
            FROM {{ chain }}_FLIPSIDE.DEFI.EZ_DEX_SWAPS t2
            WHERE t2.TX_HASH = t.TX_HASH
        )

    {% endif %}

{% endmacro %}