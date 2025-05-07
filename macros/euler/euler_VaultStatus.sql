{% macro euler_VaultStatus(chain, contract_address) %}
    with
    prices as ({{ get_multiple_coingecko_price_with_latest(chain) }})

    {% if chain not in ["bob", "berachain", "sonic"] %}
    , flipside_prices as (
        select
            hour::date as date
            , token_address as contract_address
            , avg(price) as price
            , max(decimals) as decimals
            , max(symbol) as symbol
        from {{chain}}_flipside.price.ez_prices_hourly
        group by 1, 2
    )
    {% endif %}
    
    select
        events.block_timestamp
        , events.transaction_hash
        , events.event_index
        , vaults.proxy_address as vault_address
        , vaults.asset_token_address as asset_address
        , events.decoded_log:"accumulatedFees"::float as accumulated_fees
        , events.decoded_log:"cash"::float as cash
        , events.decoded_log:"interestAccumulator"::float as interest_accumulator
        , events.decoded_log:"interestRate"::float as interest_rate
        , events.decoded_log:"timestamp"::integer as timestamp
        , events.decoded_log:"totalBorrows"::float as total_borrows
        , events.decoded_log:"totalShares"::float as total_shares
        {% if chain not in ["bob", "berachain", "sonic"] %}
        , coalesce(prices.price, flipside_prices.price) as price
        , coalesce(prices.decimals, flipside_prices.decimals) as decimals
        , coalesce(prices.symbol, flipside_prices.symbol) as symbol
        {% else %}
        , prices.price as price
        , prices.decimals as decimals
        , prices.symbol as symbol
        {% endif %}
    from {{ ref("fact_" ~ chain ~ "_decoded_events") }} events
    inner join {{ ref("fact_euler_" ~ chain ~ "_event_ProxyCreated") }} vaults 
        on lower(events.contract_address) = lower(vaults.proxy_address)
    left join prices
        on prices.date = events.block_timestamp::date
        and lower(prices.contract_address) = lower(vaults.asset_token_address)
    {% if chain not in ["bob", "berachain", "sonic"] %}
    left join flipside_prices
        on flipside_prices.date = events.block_timestamp::date
        and lower(flipside_prices.contract_address) = lower(vaults.asset_token_address)
    {% endif %}
    where events.event_name = 'VaultStatus'
    {% if is_incremental() %}
        and events.block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
{% endmacro %}