{% macro get_pendle_yt_mint_burn_events_for_chain(chain) %}

    with
        -- Get all Mint events from YT contracts
        mint_events as (
            SELECT
                block_timestamp,
                transaction_hash,
                event_index,
                contract_address as yt_address,
                'Mint' as event_type,
                DECODED_LOG:caller::STRING as caller,
                DECODED_LOG:receiverPT::STRING as receiver_pt,
                DECODED_LOG:receiverYT::STRING as receiver_yt,
                TRY_TO_NUMBER(DECODED_LOG:amountSyToMint::STRING) / 1e18 as amount_sy,
                TRY_TO_NUMBER(DECODED_LOG:amountPYOut::STRING) / 1e18 as amount_py_out,
                NULL as amount_py_to_redeem,
                NULL as amount_sy_out
            FROM {{ ref("fact_" ~ chain ~ "_decoded_events") }}
            WHERE event_name = 'Mint'
            AND DECODED_LOG:amountSyToMint IS NOT NULL
            AND DECODED_LOG:amountPYOut IS NOT NULL
            {% if is_incremental() %}
                AND block_timestamp > (select max(date(block_timestamp))-1 from {{ this }})
            {% endif %}
        ),
        
        -- Get all Burn events from YT contracts
        burn_events as (
            SELECT
                block_timestamp,
                transaction_hash,
                event_index,
                contract_address as yt_address,
                'Burn' as event_type,
                DECODED_LOG:caller::STRING as caller,
                DECODED_LOG:receiver::STRING as receiver_pt,
                NULL as receiver_yt,
                NULL as amount_sy,
                NULL as amount_py_out,
                TRY_TO_NUMBER(DECODED_LOG:amountPYToRedeem::STRING) / 1e18 as amount_py_to_redeem,
                TRY_TO_NUMBER(DECODED_LOG:amountSyOut::STRING) / 1e18 as amount_sy_out
            FROM {{ ref("fact_" ~ chain ~ "_decoded_events") }}
            WHERE event_name = 'Burn'
            AND DECODED_LOG:amountPYToRedeem IS NOT NULL
            AND DECODED_LOG:amountSyOut IS NOT NULL
            {% if is_incremental() %}
                AND block_timestamp > (select max(date(block_timestamp))-1 from {{ this }})
            {% endif %}
        ),
        
        -- Union all events
        all_events as (
            SELECT * FROM mint_events
            UNION ALL
            SELECT * FROM burn_events
        ),
        
        -- Map YT addresses to their corresponding SY addresses
        yt_to_sy_mapping as (
            SELECT 
                yt_address,
                sy_address
            FROM {{ ref("dim_pendle_" ~ chain ~ "_yt_mapping") }}
        ),
        
        -- Get SY token info with exchange rates
        sy_token_info as (
            SELECT 
                date(s.date) as date,
                s.sy_address,
                s.assetinfo_type,
                s.assetinfo_address as underlying_address,
                s.exchange_rate::number / 1e18 as exchange_rate
            FROM {{ ref("fact_pendle_sy_info") }} s
            WHERE s.chain = '{{ chain }}'
            {% if is_incremental() %}
                AND s.date > (select max(date(block_timestamp))-1 from {{ this }})
            {% endif %}
        ),
        
        -- Join events with SY mapping and info
        events_with_metadata as (
            SELECT
                e.block_timestamp,
                e.transaction_hash,
                e.event_index,
                e.yt_address,
                e.event_type,
                e.caller,
                e.receiver_pt,
                e.receiver_yt,
                e.amount_sy,
                e.amount_py_out,
                e.amount_py_to_redeem,
                e.amount_sy_out,
                m.sy_address,
                s.assetinfo_type,
                s.underlying_address,
                s.exchange_rate
            FROM all_events e
            LEFT JOIN yt_to_sy_mapping m 
                ON lower(e.yt_address) = lower(m.yt_address)
            LEFT JOIN sy_token_info s 
                ON s.sy_address = m.sy_address 
                AND date(e.block_timestamp) = s.date
            WHERE m.sy_address IS NOT NULL -- Only include events for known YT/SY pairs
        ),
        
        -- Apply exchange rate conversion and calculate relevant SY amounts
        converted_amounts as (
            SELECT
                block_timestamp,
                transaction_hash,
                event_index,
                yt_address,
                sy_address,
                underlying_address,
                event_type,
                caller,
                receiver_pt,
                receiver_yt,
                amount_py_out,
                amount_py_to_redeem,
                assetinfo_type,
                exchange_rate,
                -- For Mint events, use amount_sy; for Burn events, use amount_sy_out
                COALESCE(amount_sy, amount_sy_out) as sy_amount_raw,
                -- Apply exchange rate conversion for asset type 0
                CASE 
                    WHEN assetinfo_type = '0' THEN COALESCE(amount_sy, amount_sy_out) * exchange_rate
                    ELSE COALESCE(amount_sy, amount_sy_out)
                END as sy_amount_converted
            FROM events_with_metadata
        ),
        
        -- Add price data to convert to USD
        final_results as (
            SELECT
                c.block_timestamp,
                c.transaction_hash,
                c.event_index,
                date(c.block_timestamp) as date,
                '{{ chain }}' as chain,
                c.yt_address,
                c.sy_address,
                c.underlying_address,
                c.event_type,
                c.caller,
                c.receiver_pt,
                c.receiver_yt,
                c.amount_py_out,
                c.amount_py_to_redeem,
                p.symbol,
                c.sy_amount_raw,
                c.sy_amount_converted as sy_amount_native,
                c.sy_amount_converted * p.price as sy_amount_usd
            FROM converted_amounts c
            LEFT JOIN (
                SELECT 
                    date_trunc('day', hour) as day,
                    token_address,
                    symbol,
                    AVG(price) as price
                FROM {{ chain }}_flipside.price.ez_prices_hourly 
                GROUP BY 1, 2, 3
            ) p ON p.day = date(c.block_timestamp) AND lower(p.token_address) = lower(c.underlying_address)
        )
        
    SELECT * FROM final_results
    ORDER BY block_timestamp DESC

{% endmacro %}