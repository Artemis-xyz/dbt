{% macro get_pendle_yield_fees_for_chain_by_token(chain)%}

    with
        yt_addresses as (
            SELECT
                yt_address,
                sy_address
            FROM
                {{ ref('dim_pendle_' ~ chain ~ '_yt_mapping') }}
        ),
        
        -- Get SY tokens with their metadata and exchange rates
        sy_tokens as (
            SELECT 
                date(s.date) as date,
                s.sy_address,
                s.assetinfo_type,
                s.assetinfo_address as underlying_address,
                s.exchange_rate::number / 1e18 as exchange_rate,  -- Normalize the exchange rate 
                decimals
            FROM {{ ref("fact_pendle_sy_info") }} s
            WHERE s.chain = '{{ chain }}'
            {% if is_incremental() %}
                AND s.date > (select max(date)-1 from {{ this }})
            {% endif %}
        ),
        
        -- Raw yield fees
        raw_fees as (
            SELECT
                block_timestamp,
                tx_hash,
                contract_address as yt_address,
                yt.sy_address,
                decoded_log:amountInterestFee::number / pow(10, decimals) as fee_sy_amount
            FROM
                {{ chain }}_flipside.core.ez_decoded_event_logs l
                LEFT JOIN yt_addresses yt ON yt.yt_address = l.contract_address
                LEFT JOIN sy_tokens s on s.sy_address = yt.sy_address and l.block_timestamp::date = s.date
            WHERE event_name = 'CollectInterestFee'
                AND contract_address in (SELECT distinct yt_address FROM yt_addresses)
            {% if is_incremental() %}
                AND block_timestamp > (select max(date) from {{ this }})
            {% endif %}
        ),
        
        -- Apply exchange rate conversion to yield fees
        converted_fees as (
            SELECT
                r.block_timestamp,
                r.tx_hash,
                r.yt_address,
                r.sy_address,
                s.assetinfo_type,
                s.underlying_address,
                s.exchange_rate,
                s.decimals,
                -- Apply exchange rate conversion based on asset type
                CASE 
                    WHEN s.assetinfo_type = '0' THEN r.fee_sy_amount * s.exchange_rate
                    ELSE r.fee_sy_amount
                END as yield_fee_converted
            FROM 
                raw_fees r
                JOIN sy_tokens s ON s.sy_address = r.sy_address AND date(r.block_timestamp) = s.date
        ),
        
        -- Add price data to convert to USD
        fees_with_prices as (
            SELECT
                f.block_timestamp,
                date(f.block_timestamp) as date,
                f.tx_hash,
                f.underlying_address as token_address,
                f.yt_address,
                f.sy_address,
                p.symbol as token,
                f.yield_fee_converted as yield_fee_native,
                f.yield_fee_converted * p.price as yield_fee_usd
            FROM 
                converted_fees f
                LEFT JOIN {{ chain }}_flipside.price.ez_prices_hourly p 
                    ON p.hour = date_trunc('hour', f.block_timestamp) 
                    AND lower(p.token_address) = lower(f.underlying_address)
        )
        
    -- Final result
    SELECT
        date,
        tx_hash,
        yt_address,
        sy_address,
        token_address,
        token,
        yield_fee_usd,
        yield_fee_native
    FROM fees_with_prices
    WHERE yield_fee_usd < 1e7 -- Less than 10M USD
    
{% endmacro %}