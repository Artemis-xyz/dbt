{% macro get_solana_token_mints_burns_transfers(token_address) %}
    -- mints
    SELECT
        m.block_timestamp,
        m.tx_id,
        m.block_id,
        index,
        inner_index,
        'mint' as action,
        m.mint,
        m.token_account as tx_to_account,
        null as tx_from_account,
        m.mint_amount as amount_native,
        m.mint_authority as token_authority,
        fact_token_mint_actions_id as unique_id
    from
        solana_flipside.defi.fact_token_mint_actions m
    where
        1 = 1
        and mint = '{{ token_address }}'
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% else %}
            and block_timestamp >= '2022-11-01'
        {% endif %}

        
    union all
    
    -- transfers
    SELECT
        tf.block_timestamp,
        tf.tx_id,
        tf.block_id,
        SPLIT_PART(index, '.', 1) as index,
        COALESCE(NULLIF(SPLIT_PART(index, '.', 2), ''), 0) as inner_index,
        'transfer' as action,
        tf.mint,
        tf.tx_to as tx_to_account,
        tf.tx_from as tx_from_account,
        tf.amount as amount_native,
        null as token_authority,
        fact_transfers_id as unique_id
    from
        solana_flipside.core.fact_transfers tf
    where
        1 = 1
        and mint = '{{ token_address }}' 
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% else %}
            and block_timestamp >= '2022-11-01'
        {% endif %} 

    union all
    
    -- burns
    SELECT
        b.block_timestamp,
        b.tx_id,
        b.block_id,
        index,
        inner_index,
        'burn' as action,
        b.mint,
        null as tx_to_account,
        b.token_account as tx_from_account,
        b.burn_amount as amount_native,
        b.burn_authority as token_authority,
        fact_token_burn_actions_id as unique_id
    from
        solana_flipside.defi.fact_token_burn_actions b
    where
        1 = 1
        and mint = '{{ token_address }}' 
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% else %}
            and block_timestamp >= '2022-11-01'
        {% endif %}

{% endmacro %}