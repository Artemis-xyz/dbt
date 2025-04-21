{% macro euler_VaultStatus(chain, contract_address) %}
    with
    prices as ({{ get_multiple_coingecko_price_with_latest(chain) }})
    select
        block_timestamp
        , transaction_hash
        , event_index
        , contract_address
        , decoded_log:"accumulatedFees" as accumulated_fees
        , decoded_log:"cash" as cash
        , decoded_log:"interestAccumulator" as interest_accumulator
        , decoded_log:"interestRate" as interest_rate
        , decoded_log:"timestamp" as timestamp
        , decoded_log:"totalBorrows" as total_borrows
        , decoded_log:"totalShares" as total_shares
        , prices.price
        , prices.decimals
        , prices.symbol
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }} events
    inner join {{ ref("fact_euler_" ~ chain ~ "_event_ProxyCreated") }} vaults 
        on lower(events.contract_address) = lower(vaults.proxy_address)
    left join prices
        on prices.date = events.block_timestamp
        and lower(prices.contract_address) = lower(vaults.asset_token_address)
    {% if is_incremental() %}
        and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}