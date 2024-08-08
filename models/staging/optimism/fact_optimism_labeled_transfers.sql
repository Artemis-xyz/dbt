{{
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM",
        unique_key=["tx_hash", "index", "token_address"],
    )
}}

with
    labeled_optimism_contracts as (
        SELECT 
            address, 
            name, 
            friendly_name, 
            app, 
            chain, 
            category, 
            sub_category
        FROM {{ ref("dim_contracts_gold") }}
        WHERE chain = 'optimism'
    ),
    all_optimism_contracts as (
        SELECT 
            contract_address as address,
            c.name,
            c.friendly_name,
            c.app,
            c.chain,
            c.category,
            c.sub_category
        FROM {{ ref("dim_optimism_contract_addresses") }}
        left join labeled_optimism_contracts as c on contract_address = c.address
    ),
    chain_token_transfers as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_optimism_filtered_token_transfers"),
                    ref("fact_optimism_native_transfers"),
                ]
            )
        }}
    ),
    labeled_chain_transfers as (
        SELECT 
            block_timestamp,
            block_number,
            tx_hash,
            index,
            from_address,
            from_contracts.name as from_name,
            from_contracts.friendly_name as from_friendly_name,
            from_contracts.app as from_app,
            from_contracts.chain as from_chain,
            case 
                when from_contracts.address is not NULL and from_contracts.category is not NULL then from_contracts.category
                when from_contracts.address is not NULL and from_contracts.category is NULL then 'Unlabeled'
                else 'EOA'
            end as from_category,
            to_address,
            to_contracts.name as to_name,
            to_contracts.friendly_name as to_friendly_name,
            to_contracts.app as to_app,
            to_contracts.chain as to_chain,
            case 
                when to_contracts.category is not NULL then to_contracts.category
                when to_contracts.address is not NULL and to_contracts.category is NULL then 'Unlabeled'
                else 'EOA'
            end as to_category,
            amount,
            token_address,
            amount_usd
        FROM chain_token_transfers as t
            left join all_optimism_contracts as to_contracts on lower(t.to_address) = lower(to_contracts.address)
            left join all_optimism_contracts as from_contracts on lower(t.from_address) = lower(from_contracts.address)
        {% if is_incremental() %} 
            WHERE block_timestamp >= (
                select dateadd('day', -3, max(block_timestamp))
                from {{ this }}
            )
        {% endif %}
    )
SELECT 
    max(block_timestamp) as block_timestamp,
    max(block_number) as block_number,
    tx_hash,
    index,
    token_address,
    max_by(from_address, block_timestamp) as from_address,
    max_by(from_name, block_timestamp) as from_name,
    max_by(from_friendly_name, block_timestamp) as from_friendly_name,
    max_by(from_app, block_timestamp) as from_app,
    max_by(from_chain, block_timestamp) as from_chain,
    max_by(from_category, block_timestamp) as  from_category,
    max_by(to_address, block_timestamp) as to_address,
    max_by(to_name, block_timestamp) as to_name,
    max_by(to_friendly_name, block_timestamp) as to_friendly_name,
    max_by(to_app, block_timestamp) as to_app,
    max_by(to_chain, block_timestamp) as to_chain,
    max_by(to_category, block_timestamp) as to_category,
    max_by(amount, block_timestamp) as amount,
    max_by(amount_usd, block_timestamp) as amount_usd
FROM labeled_chain_transfers
GROUP BY 
    tx_hash, 
    index, 
    token_address
