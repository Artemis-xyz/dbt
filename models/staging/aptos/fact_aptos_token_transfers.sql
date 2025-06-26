{{ config(
    materialized="incremental", 
    snowflake_warehouse="APTOS_LG",
    unique_key=["transaction_hash", "event_index"],
    ) 
}}

WITH 
deposit_events AS (
    SELECT 
        block_number,
        tx_hash,
        block_timestamp,
        event_index AS receiving_event_index,
        account_address AS to_address,
        amount AS receiving_amount,
        token_address,
        ROW_NUMBER() OVER(PARTITION BY tx_hash, token_address ORDER BY event_index) AS rn
    FROM aptos_flipside.core.fact_transfers
    WHERE transfer_event = 'DepositEvent'
        {% if is_incremental() %}
            and block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
),
withdraw_events AS (
    SELECT 
        block_number,
        tx_hash,
        block_timestamp,
        event_index AS withdraw_event_index,
        account_address AS from_address,
        amount AS withdraw_amount,
        token_address,
        ROW_NUMBER() OVER(PARTITION BY tx_hash, token_address ORDER BY event_index) AS rn
    FROM aptos_flipside.core.fact_transfers
    WHERE transfer_event = 'WithdrawEvent'
        {% if is_incremental() %}
            and block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
),
tx_event_counts AS (
    SELECT
        tx_hash,
        token_address,
        sum(CASE WHEN transfer_event = 'DepositEvent' THEN 1 ELSE 0 END) AS deposit_count,
        sum(CASE WHEN transfer_event = 'WithdrawEvent' THEN 1 ELSE 0 END) AS withdraw_count
    FROM aptos_flipside.core.fact_transfers
    {% if is_incremental() %}
        where block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
    GROUP BY tx_hash, token_address
)
-- Number of deposit events = number of withdraw events
, case1_matches AS (
    SELECT
        w.tx_hash,
        coalesce(w.block_number, d.block_number) as block_number,
        coalesce(w.block_timestamp, d.block_timestamp) as block_timestamp,
        coalesce(w.token_address, d.token_address) as token_address,
        w.from_address,
        d.to_address,
        d.receiving_event_index as event_index,
        withdraw_amount as amount_raw
    FROM withdraw_events w
    JOIN deposit_events d ON w.tx_hash = d.tx_hash and w.rn = d.rn and w.token_address = d.token_address
    JOIN tx_event_counts c ON w.tx_hash = c.tx_hash and w.token_address = c.token_address
    WHERE c.deposit_count = c.withdraw_count
)
-- Number of deposit events > number of withdraw events
, case2_matches AS (
    SELECT
        w.tx_hash,
        coalesce(w.block_number, d.block_number) as block_number,
        coalesce(w.block_timestamp, d.block_timestamp) as block_timestamp,
        coalesce(w.token_address, d.token_address) as token_address,
        w.withdraw_event_index,
        w.from_address,
        w.withdraw_amount,
        d.receiving_event_index,
        d.to_address,
        d.receiving_amount,
        ROW_NUMBER() OVER(
            PARTITION BY d.tx_hash, d.receiving_event_index, d.token_address
            ORDER BY 
                ABS(d.receiving_amount - w.withdraw_amount),
                ABS(d.receiving_event_index - w.withdraw_event_index)
        ) as match_rank
    FROM withdraw_events w
    JOIN tx_event_counts c ON w.tx_hash = c.tx_hash and w.token_address = c.token_address
    JOIN deposit_events d ON w.tx_hash = d.tx_hash and w.token_address = d.token_address
    WHERE c.deposit_count > c.withdraw_count
)
-- Number of withdraw events > number of deposit events
, case3_matches AS (
    SELECT
        d.tx_hash,
        COALESCE(d.block_number, w.block_number) AS block_number,
        COALESCE(d.block_timestamp, w.block_timestamp) AS block_timestamp,
        COALESCE(d.token_address, w.token_address) AS token_address,
        w.withdraw_event_index,
        w.from_address,
        w.withdraw_amount,
        d.receiving_event_index,
        d.to_address,
        d.receiving_amount,
        ROW_NUMBER() OVER(
            PARTITION BY w.tx_hash, w.withdraw_event_index, w.token_address
            ORDER BY 
                ABS(d.receiving_amount - w.withdraw_amount),
                ABS(d.receiving_event_index - w.withdraw_event_index)
        ) AS match_rank
    FROM deposit_events d
    JOIN tx_event_counts c ON d.tx_hash = c.tx_hash and d.token_address = c.token_address
    JOIN withdraw_events w ON d.tx_hash = w.tx_hash and d.token_address = w.token_address
    WHERE c.withdraw_count > c.deposit_count
)

, token_transfers AS (
    select 
        block_number
        , block_timestamp
        , tx_hash
        , token_address
        , event_index
        , from_address
        , to_address
        , amount_raw
    from case1_matches
    union all
    select 
        block_number
        , block_timestamp
        , tx_hash
        , token_address
        , receiving_event_index as event_index
        , from_address
        , to_address
        , receiving_amount as amount_raw
    from case2_matches
    where match_rank = 1
    union all
    select 
        block_number
        , block_timestamp
        , tx_hash
        , token_address
        , withdraw_event_index as event_index
        , from_address
        , to_address
        , withdraw_amount as amount_raw
    from case3_matches
    where match_rank = 1
)

select 
    block_number
    , block_timestamp
    , tx_hash as transaction_hash
    , null as transaction_index
    , event_index
    , token_transfers.token_address as contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_raw / power(10, decimals) as amount_native
    , amount_native * price as amount
    , price
from token_transfers
left join aptos_flipside.price.ez_hourly_token_prices prices 
    on date_trunc('hour', token_transfers.block_timestamp) = prices.hour
    and lower(token_transfers.token_address) = lower(prices.token_address)