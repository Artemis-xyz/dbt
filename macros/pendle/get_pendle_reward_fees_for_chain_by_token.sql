{% macro get_pendle_reward_fees_for_chain_by_token(chain) %}
    
    with prices as (
        SELECT date, contract_address, symbol, avg(price) as price, avg(decimals) as decimals
        FROM ({{ get_multiple_coingecko_price_with_latest(chain)}})
        group by 1, 2, 3
    )
    
    , raw_fees as (
        SELECT
            block_timestamp,
            transaction_hash,
            l.contract_address as yt_address,
            decoded_log:rewardToken::string as token_address,
            p.symbol,
            p.decimals,
            decoded_log:amountRewardFee::number /pow(10, p.decimals) as fee_native,
            decoded_log:amountRewardFee::number /pow(10, p.decimals) * p.price as fee
        FROM
            {{ref('fact_' ~ chain ~ '_decoded_events')}} l
            LEFT JOIN prices p on lower(p.contract_address) = lower(decoded_log:rewardToken) and date(l.block_timestamp) = p.date
        WHERE event_name = 'CollectRewardFee'
            and decoded_log:amountRewardFee is not null
            and decoded_log:rewardToken is not null
    )
    
    select 
        block_timestamp,
        transaction_hash,
        yt_address,
        token_address,
        symbol,
        decimals,
        fee_native,
        fee
    from raw_fees
{% endmacro %}