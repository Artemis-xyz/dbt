{% macro get_pendle_limit_order_events_for_chain(chain, router_address='0x000000000000c9b3e2c3ec88b1b4c0cd853f4321')%}

    with
        -- Get limit order filled events from the router contract
        order_filled_logs as (
            SELECT
                block_timestamp,
                transaction_hash,
                event_index,
                DECODED_LOG:orderHash::STRING AS order_hash,
                TRY_TO_NUMBER(DECODED_LOG:orderType::STRING) AS order_type,
                DECODED_LOG:YT::STRING AS yt_address,
                DECODED_LOG:token::STRING AS token_address,
                TRY_TO_NUMBER(DECODED_LOG:netInputFromMaker::STRING) / 1e18 AS net_input_from_maker,
                TRY_TO_NUMBER(DECODED_LOG:netOutputToMaker::STRING) / 1e18 AS net_output_to_maker,
                TRY_TO_NUMBER(DECODED_LOG:feeAmount::STRING) / 1e18 AS fee_amount,
                TRY_TO_NUMBER(DECODED_LOG:notionalVolume::STRING) / 1e18 AS notional_volume,
                DECODED_LOG:maker::STRING AS maker,
                DECODED_LOG:taker::STRING AS taker
            FROM {{ ref("fact_" ~ chain ~ "_decoded_events") }}
            WHERE event_name = 'OrderFilledV2'
            AND lower(contract_address) = lower('{{ router_address }}')
            AND DECODED_LOG:notionalVolume IS NOT NULL
            {% if is_incremental() %}
                AND block_timestamp > (select max(date(block_timestamp))-1 from {{ this }})
            {% endif %}
        ),
        
        -- Map YT addresses to their corresponding SY addresses using the new mapping table
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
        
        -- Join limit order events with mapping and SY info
        limit_orders_with_metadata as (
            SELECT
                l.block_timestamp,
                l.transaction_hash,
                l.event_index,
                l.order_hash,
                l.order_type,
                l.yt_address,
                l.token_address,
                l.notional_volume,
                l.fee_amount,
                l.net_input_from_maker,
                l.net_output_to_maker,
                l.maker,
                l.taker,
                m.sy_address,
                s.assetinfo_type,
                s.underlying_address,
                s.exchange_rate
            FROM order_filled_logs l
            LEFT JOIN yt_to_sy_mapping m 
                ON lower(l.yt_address) = lower(m.yt_address)
            LEFT JOIN sy_token_info s 
                ON s.sy_address = m.sy_address 
                AND date(l.block_timestamp) = s.date
            WHERE m.sy_address IS NOT NULL -- Only include orders for known YT/SY pairs
        ),
        
        -- Apply exchange rate conversion if needed (similar to swap fees logic)
        converted_volumes as (
            SELECT
                block_timestamp,
                transaction_hash,
                event_index,
                order_hash,
                order_type,
                yt_address,
                token_address,
                sy_address,
                underlying_address,
                assetinfo_type,
                exchange_rate,
                maker,
                taker,
                net_input_from_maker,
                net_output_to_maker,
                -- Apply exchange rate conversion for asset type 0 (notionalVolume is in SY tokens)
                CASE 
                    WHEN assetinfo_type = '0' THEN notional_volume * exchange_rate
                    ELSE notional_volume
                END as volume_converted,
                CASE 
                    WHEN assetinfo_type = '0' THEN fee_amount * exchange_rate
                    ELSE fee_amount
                END as fee_converted
            FROM limit_orders_with_metadata
        ),
        
        -- Add price data to convert to USD
        final_results as (
            SELECT
                v.block_timestamp,
                v.transaction_hash,
                v.event_index,
                date(v.block_timestamp) as date,
                '{{ chain }}' as chain,
                v.order_hash,
                v.order_type,
                v.yt_address,
                v.token_address,
                v.sy_address,
                v.underlying_address,
                p.symbol,
                v.volume_converted as volume_native,
                v.fee_converted as fee_native,
                v.volume_converted * p.price as volume_usd,
                v.fee_converted * p.price as fee_usd,
                v.net_input_from_maker,
                v.net_output_to_maker,
                v.maker,
                v.taker
            FROM converted_volumes v
            LEFT JOIN (
                SELECT 
                    date_trunc('day', hour) as day,
                    token_address,
                    symbol,
                    AVG(price) as price
                FROM {{ chain }}_flipside.price.ez_prices_hourly 
                GROUP BY 1, 2, 3
            ) p ON p.day = date(v.block_timestamp) AND lower(p.token_address) = lower(v.underlying_address)
        )
        
    SELECT * FROM final_results

{% endmacro %}