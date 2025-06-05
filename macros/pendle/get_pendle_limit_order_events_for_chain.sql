{% macro get_pendle_limit_order_events_for_chain(chain, router_address='0x000000000000c9b3e2c3ec88b1b4c0cd853f4321')%}

    with
        -- Get limit order filled events from the router contract
        order_filled_logs as (
            SELECT
                block_timestamp,
                tx_hash,
                event_index,
                origin_from_address,
                DECODED_LOG:orderHash::STRING AS order_hash,
                TRY_TO_NUMBER(DECODED_LOG:orderType::STRING) AS order_type,
                DECODED_LOG:YT::STRING AS yt_address,
                DECODED_LOG:token::STRING AS token_address,
                TRY_TO_NUMBER(DECODED_LOG:netInputFromMaker::STRING) AS net_input_from_maker,
                TRY_TO_NUMBER(DECODED_LOG:netOutputToMaker::STRING) AS net_output_to_maker,
                TRY_TO_NUMBER(DECODED_LOG:feeAmount::STRING) AS fee_amount,
                TRY_TO_NUMBER(DECODED_LOG:notionalVolume::STRING) AS notional_volume,
                DECODED_LOG:maker::STRING AS maker,
                DECODED_LOG:taker::STRING AS taker
            FROM {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
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
                sy_address,
                pt_address
            FROM {{ ref("dim_pendle_" ~ chain ~ "_yt_mapping") }}
        ),
        
        -- Get SY token info with exchange rates
        sy_token_info as (
            SELECT 
                date(s.date) as date,
                s.sy_address,
                s.assetinfo_type,
                s.assetinfo_address as underlying_address,
                s.exchange_rate::number / 1e18 as exchange_rate,
                s.decimals
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
                l.origin_from_address,
                l.tx_hash,
                l.event_index,
                l.order_hash,
                l.order_type,
                l.yt_address,
                l.token_address,
                s.decimals,
                l.notional_volume as notional_volume_raw,
                l.notional_volume / pow(10, decimals) as notional_volume_native,
                l.fee_amount as fee_amount_raw,
                l.fee_amount / pow(10, decimals) as fee_amount_native,
                l.net_input_from_maker as net_input_from_maker_raw,
                l.net_input_from_maker / pow(10, decimals) as net_input_from_maker_native,
                l.net_output_to_maker as net_output_to_maker_raw,
                l.net_output_to_maker / pow(10, decimals) as net_output_to_maker_native,
                l.maker,
                l.taker,
                m.sy_address,
                m.pt_address,
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
                origin_from_address,
                tx_hash,
                event_index,
                order_hash,
                order_type,
                yt_address,
                token_address,
                sy_address,
                pt_address,
                underlying_address,
                assetinfo_type,
                exchange_rate,
                decimals,
                maker,
                taker,
                net_input_from_maker_native,
                net_output_to_maker_native,
                -- Apply exchange rate conversion for asset type 0 (notionalVolume is in SY tokens)
                CASE 
                    WHEN assetinfo_type = '0' THEN notional_volume_native * exchange_rate
                    ELSE notional_volume_native
                END as volume_native,
                CASE 
                    WHEN assetinfo_type = '0' THEN fee_amount_native * exchange_rate
                    ELSE fee_amount_native
                END as fee_native,
                CASE 
                    WHEN assetinfo_type = '0' THEN notional_volume_native * exchange_rate
                    ELSE notional_volume_native
                END as volume_usd,
                CASE 
                    WHEN assetinfo_type = '0' THEN fee_amount_native * exchange_rate
                    ELSE fee_amount_native
                END as fee_usd
            FROM limit_orders_with_metadata
        )
        , prices as (
            SELECT
                date,
                lower(contract_address) as contract_address,
                lower(symbol) as symbol,
                avg(price) as price
            FROM ({{ get_multiple_coingecko_price_with_latest(chain)}})
            GROUP BY 1, 2, 3
        )
        -- Add price data to convert to USD
        SELECT
            v.block_timestamp,
            v.origin_from_address,
            v.tx_hash,
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
            v.volume_native,
            v.fee_native,
            v.volume_native * p.price as volume,
            v.fee_native * p.price as fee,
            v.net_input_from_maker_native,
            v.net_output_to_maker_native,
            v.maker,
            v.taker,
            m.market_address,
            m.pt_address
        FROM converted_volumes v
        LEFT JOIN prices p ON p.date = date(v.block_timestamp) AND lower(p.contract_address) = lower(v.underlying_address)
        LEFT JOIN {{ ref("dim_pendle_" ~ chain ~ "_market_metadata") }} m ON m.pt_address = v.pt_address

{% endmacro %}