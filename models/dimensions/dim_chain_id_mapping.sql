with chain_ids as (
    select 
        chain as chain_name,
        id as native_chain_id
    from {{ ref('chain_ids') }}
),

stargate_ids as (
    select 
        chain as chain_name,
        id as stargate_chain_id
    from {{ ref('stargate_chain_ids') }}
),

chain_agnostic_ids as (
    select 
        chain as chain_name,
        chain_agnostic_id
    from {{ ref('chain_agnostic_ids') }}
),

cctp_ids as (
    select 
        chain as chain_name,
        chain_id as cctp_chain_id
    from {{ ref('cctp_chain_ids') }}
),

wormhole_ids as (
    select 
        chain as chain_name,
        id as wormhole_chain_id
    from {{ ref('wormhole_chain_ids') }}
),

coingecko_ids as (
    select 
        chain as chain_name,
        coingecko_id as native_token_coingecko_id
    from {{ ref('native_token_coingecko_id_seed') }}
),

unified_chains as (
    select distinct chain_name from (
        select chain_name from chain_ids
        union all
        select chain_name from stargate_ids
        union all
        select chain_name from chain_agnostic_ids
        union all
        select chain_name from cctp_ids
        union all
        select chain_name from wormhole_ids
        union all
        select chain_name from coingecko_ids
    )
)

select 
    uc.chain_name,
    ci.native_chain_id,
    si.stargate_chain_id,
    cai.chain_agnostic_id,
    cctp.cctp_chain_id,
    wi.wormhole_chain_id,
    cgi.native_token_coingecko_id
from unified_chains uc
left join chain_ids ci 
    on uc.chain_name = ci.chain_name
left join stargate_ids si 
    on uc.chain_name = si.chain_name
left join chain_agnostic_ids cai 
    on uc.chain_name = cai.chain_name
left join cctp_ids cctp 
    on uc.chain_name = cctp.chain_name
left join wormhole_ids wi 
    on uc.chain_name = wi.chain_name
left join coingecko_ids cgi 
    on uc.chain_name = cgi.chain_name