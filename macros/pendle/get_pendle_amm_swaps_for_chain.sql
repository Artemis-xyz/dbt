{% macro get_pendle_amm_swaps_for_chain(chain, blacklist=(''))%}

    with
        swap_logs as (
            SELECT
                block_timestamp,
                {% if chain!='optimism'%}transaction_hash{% else %}tx_hash{% endif %}  as tx_hash,
                event_index,
                DECODED_LOG:caller::STRING AS caller,
                TRY_TO_NUMBER(DECODED_LOG:netPtOut::STRING) AS netPtOut_raw,
                TRY_TO_NUMBER(DECODED_LOG:netSyFee::STRING) AS netSyFee_raw,
                ABS(TRY_TO_NUMBER(DECODED_LOG:netSyOut::STRING)) AS netSyOut_raw,
                TRY_TO_NUMBER(DECODED_LOG:netSyToReserve::STRING) AS netSyToReserve_raw,
                DECODED_LOG:receiver::STRING AS receiver,
                contract_address as market_address
            {% if chain!='optimism' %}
                FROM {{ref('fact_' ~chain~'_decoded_events')}}
            {% else %}
                FROM {{ chain }}_flipside.core.ez_decoded_event_logs
            {% endif %}
            WHERE event_name = 'Swap'
            AND DECODED_LOG:netSyFee IS NOT NULL
            AND DECODED_LOG:netSyToReserve IS NOT NULL
            {% if is_incremental() %}
                AND block_timestamp > (select max(date)-1 from {{ this }})
            {% endif %}
            {% if blacklist is string %} AND lower(contract_address) != '{{ blacklist }}'
            {% elif blacklist | length > 1 %} AND lower(contract_address) not in {{ blacklist }}
            {% endif %}
        ),
        
        -- Get SY tokens with their metadata and exchange rates
        sy_tokens as (
            SELECT 
                date(s.date) as date,
                s.sy_address,
                s.assetinfo_type,
                CASE WHEN s.assetinfo_address = '0x0000000000000000000000000000000000000000'
                    THEN {{ get_native_token_chain_agnostic_namespace_for_chain(chain)}}
                    ELSE s.assetinfo_address
                END as underlying_address,
                s.exchange_rate / pow(10, 18) as exchange_rate,  -- Normalize the exchange rate 
                decimals
            FROM {{ ref("fact_pendle_sy_info") }} s
            WHERE s.chain = '{{ chain }}'
            {% if is_incremental() %}
                AND s.date > (select max(date)-1 from {{ this }})
            {% endif %}
        ),
        
        -- Map markets to their SY tokens
        market_metadata as (
            SELECT 
                m.market_address,
                m.sy_address,
                y.yt_address,
                y.pt_address
            FROM {{ ref("dim_pendle_" ~ chain ~ "_market_metadata") }} m
            LEFT JOIN {{ ref("dim_pendle_" ~ chain ~ "_yt_mapping") }} y ON m.pt_address = y.pt_address
            WHERE y.pt_address IS NOT NULL
        ),
        
        -- Calculate daily swap fees and apply exchange rate conversion
        swaps_with_metrics as (
            SELECT
                date(l.block_timestamp) as date,
                l.block_timestamp,
                l.tx_hash,
                l.event_index,
                m.sy_address,
                m.yt_address,
                m.pt_address,
                l.market_address,
                s.assetinfo_type,
                s.underlying_address,
                s.exchange_rate,
                -- Calculate supply side fees (excluding revenue fee)
                (l.netSyFee_raw - l.netSyToReserve_raw) / pow(10, decimals) as supply_side_fees_native,
                -- Revenue is tracked by netSyToReserve
                l.netSyToReserve_raw / pow(10, decimals) as revenue_native,
                -- Total fees
                l.netSyFee_raw / pow(10, decimals) as total_fees_native,
                -- Net Sy Out
                l.netSyOut_raw / pow(10, decimals) as net_sy_out_native
            FROM swap_logs l
            JOIN market_metadata m ON m.market_address = l.market_address
            JOIN sy_tokens s ON s.sy_address = m.sy_address AND date(l.block_timestamp) = s.date
        ),
        
        -- Apply exchange rate conversion for asset type 0
        adjusted_swaps_with_exchange_rate as (
            SELECT
                date,
                block_timestamp,
                tx_hash,
                event_index,
                sy_address,
                yt_address,
                pt_address,
                market_address,
                underlying_address,
                CASE 
                    WHEN assetinfo_type = '0' THEN supply_side_fees_native * exchange_rate
                    ELSE supply_side_fees_native
                END as supply_side_fees_native,
                CASE 
                    WHEN assetinfo_type = '0' THEN revenue_native * exchange_rate
                    ELSE revenue_native
                END as revenue_native,
                CASE 
                    WHEN assetinfo_type = '0' THEN total_fees_native * exchange_rate
                    ELSE total_fees_native
                END as total_fees_native,
                CASE 
                    WHEN assetinfo_type = '0' THEN net_sy_out_native * exchange_rate
                    ELSE net_sy_out_native
                END as net_sy_out_native
            FROM swaps_with_metrics
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
            f.block_timestamp,
            f.tx_hash,
            f.event_index,
            f.date,
            '{{ chain }}' as chain,
            f.yt_address,
            f.pt_address,
            f.sy_address,
            f.market_address,
            p.symbol,
            f.underlying_address as token_address,
            f.supply_side_fees_native,
            f.revenue_native,
            f.total_fees_native,
            f.net_sy_out_native,
            p.price,
            f.supply_side_fees_native * p.price as supply_side_fees_usd,
            f.revenue_native * p.price as revenue_usd,
            f.total_fees_native * p.price as total_fees_usd,
            f.net_sy_out_native * p.price as net_sy_out_usd
        FROM adjusted_swaps_with_exchange_rate f
        LEFT JOIN prices p ON p.date = f.date AND lower(p.contract_address) = lower(f.underlying_address)
    
{% endmacro %}