{% macro rolling_active_addresses(chain, model_version='') %}
    with
    {% if chain == 'solana' %}
            distinct_dates as (
                select distinct 
                    raw_date
                from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
                where succeeded = 'TRUE'
                {% if is_incremental() %}
                    and raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
                {% endif %}
            ),
            distinct_dates_for_rolling_active_address as (
                select distinct 
                    raw_date,
                    value as from_address 
                from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}, lateral flatten(input => signers)
                where succeeded = 'TRUE'
            ),
    {% elif chain == 'sui' %}
        distinct_dates as (
            select distinct
                raw_date
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                raw_date,
                sender as from_address
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
        ),
    {% elif chain == 'zksync' %}
        distinct_dates as (
            select distinct
                block_date as raw_date
            from zksync_dune.zksync.transactions
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_date as raw_date,
                "FROM" as from_address
            from zksync_dune.zksync.transactions
        ),
    {% elif chain == 'acala' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_acala_uniq_daily_signers") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                signer as from_address
            from {{ ref("fact_acala_uniq_daily_signers") }}
        ),
    {% elif chain == 'fantom' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_fantom_uniq_daily_addresses") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                from_address as from_address
            from {{ ref("fact_fantom_uniq_daily_addresses") }}
        ),
    {% elif chain == 'polkadot' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_polkadot_uniq_daily_signers") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                signer_pub_key as from_address
            from {{ ref("fact_polkadot_uniq_daily_signers") }}
        ),
    {% elif chain == 'stride' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_stride_uniq_daily_senders") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                sender as from_address
            from {{ ref("fact_stride_uniq_daily_senders") }}
        ),
    {% elif chain == 'bitcoin' %}
        distinct_dates as (
            select distinct
                block_timestamp::date as raw_date
            from bitcoin_flipside.core.fact_transactions
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_timestamp::date as raw_date
                , value:scriptPubKey:address::string as from_address
            from bitcoin_flipside.core.fact_transactions,
            lateral flatten(input => outputs)
            where from_address is not null
        ),
    {% elif chain == 'gnosis' %}
        distinct_dates as (
            select distinct
                block_timestamp::date as raw_date
            from gnosis_flipside.core.fact_transactions
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_timestamp::date as raw_date
                , from_address as from_address
            from gnosis_flipside.core.fact_transactions
            where from_address is not null
        ),
    {% elif chain == 'sei_evm' %}
        distinct_dates as (
            select 
                block_timestamp::date as raw_date
            from sei_flipside.core_evm.fact_transactions
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_timestamp::date as raw_date
                , from_address as from_address
            from sei_flipside.core_evm.fact_transactions
            where from_address is not null
        ),
    {% elif chain == 'sei_wasm' %}
        distinct_dates as (
            select 
                block_timestamp::date as raw_date
            from sei_flipside.core.fact_transactions
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_timestamp::date as raw_date
                , tx_from as from_address
            from sei_flipside.core.fact_transactions
            where tx_from is not null
        ),
    {% elif chain == 'starknet' %}
        distinct_dates as (
            select distinct
                block_timestamp::date as raw_date
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_timestamp::date as raw_date
                , from_address as from_address
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
            where from_address is not null
        ),
    {% elif chain == 'celo' %}
        distinct_dates as (
            select distinct
                block_timestamp::date as raw_date
            from {{ ref("fact_" ~ chain ~ "_transactions")}}  
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_timestamp::date as raw_date
                , from_address as from_address
            from {{ref("fact_" ~ chain ~ "_transactions")}}
        ),
    {% elif chain == 'linea' %}
        distinct_dates as (
            select distinct
                CAST(TO_TIMESTAMP(FACT_LINEA_TRANSACTIONS.BLOCK_TIMESTAMP) AS DATE) as raw_date
            from {{ ref("fact_linea_transactions") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                CAST(TO_TIMESTAMP(FACT_LINEA_TRANSACTIONS.BLOCK_TIMESTAMP) AS DATE) as raw_date
                , from_address as from_address
            from {{ ref("fact_linea_transactions") }}
        ),
    {% elif chain == 'scroll' %}
        distinct_dates as (
            select distinct
                CAST(TO_TIMESTAMP(BLOCK_TIMESTAMP) AS DATE) as raw_date
            from {{ ref("fact_scroll_transactions")}}  
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                CAST(TO_TIMESTAMP(BLOCK_TIMESTAMP) AS DATE) as raw_date
                , from_address as from_address
            from {{ref("fact_scroll_transactions")}}
        ),
    {% elif chain == 'aptos' %}
        single_signed_transactions as (
            select block_timestamp::date as date, parse_json(signature):public_key as signer
            from aptos_flipside.core.fact_transactions
            where
                tx_type = 'user_transaction'
                and success = 'true'
                and parse_json(signature):public_key is not null
                {% if is_incremental() %}
                    and block_timestamp::date > (select dateadd('day', -30, max(date)) from {{ this }})
                {% endif %}
        ),
        primary_multi_signed_transactions as (
            select
                block_timestamp::date as date,
                parse_json(signature):sender:public_key as signer
            from aptos_flipside.core.fact_transactions
            where
                tx_type = 'user_transaction'
                and success = 'true'
                and parse_json(signature):sender is not null
                {% if is_incremental() %}
                    and block_timestamp::date > (select dateadd('day', -30, max(date)) from {{ this }})
                {% endif %}
        ),
        raw_secondary_multi_signed_transactions as (
            select
                block_timestamp,
                parse_json(signature):secondary_signers as secondary_signers
            from aptos_flipside.core.fact_transactions
            where
                tx_type = 'user_transaction'
                and success = 'true'
                and parse_json(signature):secondary_signers is not null
                {% if is_incremental() %}
                    and block_timestamp::date > (select dateadd('day', -30, max(date)) from {{ this }})
                {% endif %}
        ),
        secondary_multi_signed_transactions as (
            select block_timestamp::date as date, value:"public_key" as signer
            from
                raw_secondary_multi_signed_transactions,
                lateral flatten(input => secondary_signers)
            where value:"public_key" is not null
        ),
        raw_bitmap_multi_sig_transactions as (
            select block_timestamp::date as date, value:"public_keys" as public_keys
            from
                raw_secondary_multi_signed_transactions,
                lateral flatten(input => secondary_signers)
        ),
        bitmap_multi_sig_transactions as (
            select date, value as signer
            from raw_bitmap_multi_sig_transactions, lateral flatten(input => public_keys)
        ),
        combined_signers as (
            select date, signer
            from single_signed_transactions
            union all
            select date, signer
            from primary_multi_signed_transactions
            union all
            select date, signer
            from secondary_multi_signed_transactions
            union all
            select date, signer
            from bitmap_multi_sig_transactions
        ),
        distinct_dates as (
            select distinct
                date as raw_date
            from combined_signers
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date
                , signer as from_address
            from combined_signers   
        ),
    {% elif chain == 'zora' %}
        distinct_dates as (
            select 
                CAST(TO_TIMESTAMP(BLOCK_TIMESTAMP) AS DATE) as raw_date
            from {{ ref("fact_zora_transactions")}}  
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select 
                CAST(TO_TIMESTAMP(BLOCK_TIMESTAMP) AS DATE) as raw_date
                , from_address as from_address
            from {{ref("fact_zora_transactions")}}
        ),
    {% else %}
        distinct_dates as (
            select distinct 
                raw_date
            from {{ ref("fact_" ~ chain ~ "_transactions" ~ (model_version)) }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct 
                raw_date,
                from_address
            from {{ ref("fact_" ~ chain ~ "_transactions" ~ (model_version)) }}
        ),
    {% endif %}


    rolling_mau as (
        select 
            t1.raw_date,
            coalesce(count(distinct t2.from_address), 0) as mau
        from distinct_dates t1
        join distinct_dates_for_rolling_active_address t2 on t2.raw_date between dateadd(DAY, -29, t1.raw_date) and t1.raw_date
        group by t1.raw_date
    ),
    rolling_wau as (
        select 
            t1.raw_date,
            coalesce(count(distinct t2.from_address), 0) as wau
        from distinct_dates t1
        join distinct_dates_for_rolling_active_address t2 on t2.raw_date between dateadd(DAY, -6, t1.raw_date) and t1.raw_date
        group by t1.raw_date
    )
select 
    rolling_mau.raw_date as date,
    '{{ chain }}' as chain,
    mau,
    wau
from rolling_mau
left join rolling_wau using(raw_date)
where rolling_mau.raw_date < to_date(sysdate())
order by date
{% endmacro %}