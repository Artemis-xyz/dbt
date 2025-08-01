{{ config(materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

with source_data as (
    select *
    from {{ source('PC_DBT_DB', 'fact_base_decoded_events') }}
    where contract_address ilike '0x777777751622c0d3258f214f9df38e35bf45baf3'
        and event_name in ('CoinCreated', 'CoinCreatedV4', 'CreatorCoinCreated')
        {% if is_incremental() %}
            and block_timestamp >= (select max(block_timestamp) from {{ this }})
        {% endif %}
),

parsed_events as (
    select
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        event_index,
        contract_address as factory_address,
        event_name,
        topic_zero,
        decoded_log_status,
        try_parse_json(decoded_log):caller::string as caller_address,
        try_parse_json(decoded_log):payoutRecipient::string as payout_recipient_address,
        try_parse_json(decoded_log):platformReferrer::string as platform_referrer_address,
        try_parse_json(decoded_log):currency::string as currency_address,
        try_parse_json(decoded_log):coin::string as coin_address,
        try_parse_json(decoded_log):name::string as coin_name,
        try_parse_json(decoded_log):symbol::string as coin_symbol,
        try_parse_json(decoded_log):uri::string as metadata_uri,
        try_parse_json(decoded_log):version::string as protocol_version,
        -- Pool-related fields (only for V4 events)
        case when event_name in ('CoinCreatedV4', 'CreatorCoinCreated') 
             then try_parse_json(decoded_log):poolKey:currency0::string end as pool_currency0,
        case when event_name in ('CoinCreatedV4', 'CreatorCoinCreated') 
             then try_parse_json(decoded_log):poolKey:currency1::string end as pool_currency1,
        case when event_name in ('CoinCreatedV4', 'CreatorCoinCreated') 
             then try_parse_json(decoded_log):poolKey:fee::number end as pool_fee,
        case when event_name in ('CoinCreatedV4', 'CreatorCoinCreated') 
             then try_parse_json(decoded_log):poolKey:hooks::string end as pool_hooks,
        case when event_name in ('CoinCreatedV4', 'CreatorCoinCreated') 
             then try_parse_json(decoded_log):poolKey:tickSpacing::number end as pool_tick_spacing,
        case when event_name in ('CoinCreatedV4', 'CreatorCoinCreated') 
             then try_parse_json(decoded_log):poolKeyHash::string end as pool_key_hash,
        -- Pool address logic
        case 
            when event_name = 'CoinCreated' then try_parse_json(decoded_log):pool::string
            when event_name in ('CoinCreatedV4', 'CreatorCoinCreated') then try_parse_json(decoded_log):poolKey:currency0::string
        end as pool_address,
        topic_data,
        data as raw_data,
        decoded_log,
        event_info
    from source_data
    where decoded_log_status = true
)

select
    transaction_hash,
    block_number,
    block_timestamp,
    transaction_index,
    event_index,
    factory_address,
    event_name,
    topic_zero,
    decoded_log_status,
    caller_address,
    payout_recipient_address,
    platform_referrer_address,
    currency_address,
    coin_address,
    pool_address,
    pool_key_hash,
    pool_currency0,
    pool_currency1,
    pool_fee,
    pool_hooks,
    pool_tick_spacing,
    coin_name,
    coin_symbol,
    metadata_uri,
    protocol_version,
    -- Simplified currency type logic
    case 
        when currency_address = '0x4200000000000000000000000000000000000006' then 'WETH'
        when currency_address = '0x0000000000000000000000000000000000000000' then 'ETH'
        when currency_address is not null then 'OTHER'
    end as currency_type,
    -- Simplified metadata type logic
    case 
        when metadata_uri like 'ipfs://%' then 'IPFS'
        when metadata_uri like 'http://%' or metadata_uri like 'https://%' then 'HTTP'
        when metadata_uri is not null then 'OTHER'
    end as metadata_type,
    -- Simplified event version mapping
    case event_name
        when 'CoinCreated' then 'V1'
        when 'CoinCreatedV4' then 'V4'
        when 'CreatorCoinCreated' then 'V4_CREATOR'
    end as event_version,
    'COIN_CREATION' as event_category,
    -- Simplified coin type mapping
    case event_name
        when 'CoinCreated' then 'STANDARD'
        when 'CoinCreatedV4' then 'V4_STANDARD'
        when 'CreatorCoinCreated' then 'CREATOR'
    end as coin_type,
    topic_data,
    raw_data,
    decoded_log,
    event_info
from parsed_events
order by block_timestamp desc, event_index desc