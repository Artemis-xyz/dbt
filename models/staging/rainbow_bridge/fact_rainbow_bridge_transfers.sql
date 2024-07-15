{{ config(
    materialized="incremental",
) }}
WITH functioncall AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        method_name,
        args,
        logs,
        receiver_id,
        signer_id,
        receipt_succeeded
    from near_flipside.core.fact_actions_events_function_call
    {% if is_incremental() %}
        where block_timestamp >= (select max(block_timestamp) from {{ this }})
    {% endif %}
),
near_to_aurora AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        LPAD(
            IFF(len(SPLIT(args :msg :: STRING, ':') [1]) = 104, SUBSTR(args :msg :: STRING, -40), args :msg :: STRING),
            42,
            '0x'
        ) AS destination_address,
        signer_id AS source_address,
        'aurora' AS destination_chain,
        'near' AS source_chain,
        receipt_succeeded,
        method_name,
        'aurora' AS bridge_address,
    FROM
        functioncall
    WHERE
        method_name = 'ft_transfer_call'
        AND args :receiver_id :: STRING = 'aurora'
        AND (
            receiver_id = 'aurora'
            OR receiver_id LIKE '%.factory.bridge.near'
        )
),
aurora_to_near AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receiver_id AS token_address,
        args :amount :: INT AS amount_raw,
        args :receiver_id :: STRING AS destination_address,
        'near' AS destination_chain,
        'aurora' AS source_chain,
        receipt_succeeded,
        method_name,
        args,
    FROM
        functioncall
    WHERE
        method_name = 'ft_transfer'
        AND signer_id = 'relay.aurora'
        AND NOT (
            -- Exclude 1 NEAR fee for fast bridge
            signer_id = 'relay.aurora'
            AND receiver_id = 'wrap.near'
            AND args :receiver_id :: STRING IN (
                '74abd625a1132b9b3258313a99828315b10ef864.aurora',
                '055707c67977e8217f98f19cfa8aca18b2282d0c.aurora',
                'e0302be5963b1f13003ab3a4798d2853bae731a7.aurora'
            )
        )
),
aurora_to_near_src_address AS (
    SELECT
        tx_hash,
        REGEXP_SUBSTR(
            logs [0] :: STRING,
            '0x[0-9a-fA-F]{40}'
        ) AS source_address
    FROM
        functioncall
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                aurora_to_near
        )
        AND method_name = 'submit'
),
aurora_to_near_final AS (
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_hash,
        A.token_address,
        A.amount_raw,
        A.destination_address,
        b.source_address,
        A.destination_chain,
        A.source_chain,
        A.receipt_succeeded,
        A.method_name
    FROM
        aurora_to_near A
        LEFT JOIN aurora_to_near_src_address b
        ON A.tx_hash = b.tx_hash
),
near_to_aurora_raw_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        destination_address as recipient,
        source_address as depositor,
        destination_chain,
        source_chain,
        receipt_succeeded
    FROM
        near_to_aurora
    UNION ALL
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        token_address,
        amount_raw,
        destination_address as recipient,
        source_address as depositor,
        destination_chain,
        source_chain,
        receipt_succeeded
    FROM
        aurora_to_near_final
),
near_prices as (
    select 
        hour::date as date,
        token_address,
        decimals,
        avg(price) as price
    from near_flipside.price.ez_prices_hourly
    group by 1, 2, 3
),
near_aurora_token_transfers as (
    select
        block_timestamp,
        tx_hash,
        source_chain,
        destination_chain,
        recipient,
        depositor,
        near_to_aurora_raw_transfers.token_address,
        amount_raw as amount,
        coalesce(amount_raw / power(10, decimals) * price, 0) as amount_usd
    from near_to_aurora_raw_transfers
    left join near_prices on near_prices.date = block_timestamp::date
        and lower(near_to_aurora_raw_transfers.token_address) = lower(near_prices.token_address)
    where tx_hash != 'HaK2Rft7UzAqHk3CHscWnEeWW8Hisupqtfe7zf82GRLD' -- incorrect pricing for this transfer
),
ethereum_near_raw_transfers as (
    select
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address as depositor,
        origin_from_address as recipient, -- assume it is the same person recievied the token on the other end (may need to adjust)
        coalesce(decoded_log:"amount"::bigint, decoded_log:"value"::bigint, decoded_log:"wad"::bigint, decoded_log:"_value"::bigint, decoded_log:"_amount"::bigint, decoded_log:"tokens"::bigint)  as amount,
        contract_address as token_address,
        'ethereum' as source_chain,
        'near' as destination_chain,
        decoded_log
    from ethereum_flipside.core.ez_decoded_event_logs
    where origin_to_address='0x23ddd3e3692d1861ed57ede224608875809e127f'
        and origin_function_signature='0x0889bfe7'
        and event_name='Transfer'
        {% if is_incremental() %}
            and block_timestamp > (select max(block_timestamp) from {{ this }})
        {% endif %}
    union all
    select 
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address as depositor,
        origin_from_address as recipient, -- assume it is the same person recievied the token on the other end (may need to adjust)
        coalesce(decoded_log:"amount"::bigint, decoded_log:"value"::bigint, decoded_log:"wad"::bigint, decoded_log:"_value"::bigint, decoded_log:"_amount"::bigint, decoded_log:"tokens"::bigint)  as amount,
        contract_address as token_address,
        'near' as source_chain,
        'ethereum' as destination_chain,
        decoded_log
    from ethereum_flipside.core.ez_decoded_event_logs
    where origin_to_address='0x23ddd3e3692d1861ed57ede224608875809e127f'
        and origin_function_signature='0x4a00c629'
        and event_name='Transfer'
        {% if is_incremental() %}
            and block_timestamp > (select max(block_timestamp) from {{ this }})
        {% endif %}
),
ethereum_to_near_recipient as (
    select
        tx_hash,
        decoded_log:"accountId"::string as depositor
    from ethereum_flipside.core.ez_decoded_event_logs
    where origin_to_address='0x23ddd3e3692d1861ed57ede224608875809e127f'
        and origin_function_signature='0x0889bfe7'
        and event_name='Locked'
        {% if is_incremental() %}
            and block_timestamp > (select max(block_timestamp) from {{ this }})
        {% endif %}
),
near_ethereum_token_transfers as (
    select 
        block_timestamp,
        t.tx_hash,
        source_chain,
        destination_chain,
        recipient,
        coalesce(r.depositor, t.depositor) as depositor,
        t.token_address,
        amount,
        coalesce((amount / power(10, p.decimals)) * price, 0) as amount_usd
    from ethereum_near_raw_transfers t
    left join ethereum_to_near_recipient r  on t.tx_hash = r.tx_hash
    left join ethereum_flipside.price.ez_prices_hourly p
        on date_trunc('hour', t.block_timestamp) = p.hour
        and lower(t.token_address) = lower(p.token_address)
)

select
    block_timestamp,
    tx_hash,
    source_chain,
    destination_chain,
    recipient,
    depositor,
    token_address,
    amount,
    amount_usd
from near_ethereum_token_transfers
union all
select 
    block_timestamp,
    tx_hash,
    source_chain,
    destination_chain,
    recipient,
    depositor,
    token_address,
    amount,
    amount_usd
from near_aurora_token_transfers