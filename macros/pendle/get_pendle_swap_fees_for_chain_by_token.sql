{% macro get_pendle_swap_fees_for_chain_by_token(chain, blacklist=(''))%}

    with
        swap_logs as (
            SELECT
                block_timestamp,
                tx_hash,
                DECODED_LOG:caller::STRING AS caller,
                TRY_TO_NUMBER(DECODED_LOG:netPtOut::STRING) / 1e18 AS netPtOut,
                TRY_TO_NUMBER(DECODED_LOG:netSyFee::STRING) / 1e18 AS netSyFee,
                ABS(TRY_TO_NUMBER(DECODED_LOG:netSyOut::STRING) / 1e18) AS netSyOut,
                TRY_TO_NUMBER(DECODED_LOG:netSyToReserve::STRING) / 1e18 AS netSyToReserve,
                DECODED_LOG:receiver::STRING AS receiver,
                contract_address as market_address
            FROM {{ chain }}_flipside.core.ez_decoded_event_logs
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
                s.assetinfo_address as underlying_address,
                s.exchange_rate / pow(10, decimals) as exchange_rate  -- Normalize the exchange rate 
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
                m.sy_address
            FROM {{ ref("dim_pendle_" ~ chain ~ "_market_metadata") }} m
        ),
        
        -- Calculate daily swap fees and apply exchange rate conversion
        daily_fees as (
            SELECT
                date(l.block_timestamp) as date,
                m.sy_address,
                s.assetinfo_type,
                s.underlying_address,
                s.exchange_rate,
                -- Calculate supply side fees (excluding revenue fee, similar to TypeScript code)
                SUM(l.netSyFee - l.netSyToReserve) as supply_side_fees_raw,
                -- Revenue is tracked by netSyToReserve
                SUM(l.netSyToReserve) as revenue_raw,
                -- Total fees
                SUM(l.netSyFee) as total_fees_raw,
                -- Net Sy Out
                SUM(l.netSyOut) as net_sy_out_raw
            FROM swap_logs l
            JOIN market_metadata m ON m.market_address = l.market_address
            JOIN sy_tokens s ON s.sy_address = m.sy_address AND date(l.block_timestamp) = s.date
            GROUP BY 1, 2, 3, 4, 5
        ),
        
        -- Apply exchange rate conversion for asset type 0, similar to the TypeScript code
        converted_fees as (
            SELECT
                date,
                sy_address,
                underlying_address,
                CASE 
                    WHEN assetinfo_type = '0' THEN supply_side_fees_raw * exchange_rate
                    ELSE supply_side_fees_raw
                END as supply_side_fees,
                CASE 
                    WHEN assetinfo_type = '0' THEN revenue_raw * exchange_rate
                    ELSE revenue_raw
                END as revenue,
                CASE 
                    WHEN assetinfo_type = '0' THEN total_fees_raw * exchange_rate
                    ELSE total_fees_raw
                END as total_fees,
                CASE 
                    WHEN assetinfo_type = '0' THEN net_sy_out_raw * exchange_rate
                    ELSE net_sy_out_raw
                END as net_sy_out
            FROM daily_fees
        ),
        
        -- Add price data to convert to USD
        fees_with_prices as (
            SELECT
                f.date,
                '{{ chain }}' as chain,
                p.symbol,
                f.underlying_address,
                f.supply_side_fees,
                f.revenue,
                f.total_fees,
                f.net_sy_out,
                p.price,
                f.supply_side_fees * p.price as supply_side_fees_usd,
                f.revenue * p.price as revenue_usd,
                f.total_fees * p.price as total_fees_usd,
                f.net_sy_out * p.price as net_sy_out_usd
            FROM converted_fees f
            LEFT JOIN (
                SELECT 
                    date_trunc('day', hour) as day,
                    token_address,
                    symbol,
                    AVG(price) as price
                FROM {{ chain }}_flipside.price.ez_prices_hourly 
                GROUP BY 1, 2, 3
            ) p ON p.day = f.date AND lower(p.token_address) = lower(f.underlying_address)
        )
        
    -- Final aggregation
    SELECT
        date,
        chain,
        symbol,
        SUM(total_fees_usd) as fee_usd,
        SUM(total_fees) as fee_native,
        SUM(net_sy_out_usd) as volume_usd,
        SUM(net_sy_out) as volume_native,
        SUM(revenue_usd) as revenue_usd,
        SUM(revenue) as revenue_native,
        SUM(supply_side_fees_usd) as supply_side_fees_usd,
        SUM(supply_side_fees) as supply_side_fees_native
    FROM fees_with_prices
    GROUP BY 1, 2, 3

{% endmacro %}