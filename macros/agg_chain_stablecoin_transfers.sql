-- Assumes the following:
-- 1. The table `fact_{{ chain }}_stablecoin_contracts` exists
-- 2. The table `{{ chain }}_flipside.core.ez_decoded_event_logs` exists and the
-- contracts in `fact_{{ chain }}_stablecoin_contracts` are decoded
-- 3. The table `{{ chain }}_flipside.core.fact_transactions` exists
{% macro agg_chain_stablecoin_transfers(chain) %}
    -- TRON Special case - comes from Allium
    {% if chain in ("tron") %}
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            block_number,
            unique_id as index,
            transaction_hash as tx_hash,
            from_address,
            to_address,
            -- NULL address on TRON is different
            from_address = 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb' as is_mint,
            to_address = 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb' as is_burn,
            coalesce(amount, 0) as amount,
            case
                when is_mint then amount when is_burn then -1 * amount else 0
            end as inflow,
            case
                when not is_mint and not is_burn then amount else 0
            end as transfer_volume,
            token_address as contract_address,
            fact_{{ chain }}_stablecoin_contracts.symbol
        from tron_allium.assets.trc20_token_transfers
        join
            fact_{{ chain }}_stablecoin_contracts
            on lower(tron_allium.assets.trc20_token_transfers.token_address)
            = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
        where
            lower(tron_allium.assets.trc20_token_transfers.token_address) in (
                select lower(contract_address)
                from fact_{{ chain }}_stablecoin_contracts
            )
    -- TODO: Refactor to support native currencies. Currently assumes everything is $1
    -- b/c of perf issues when joining
    {% elif chain in ("solana") %}
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            tx_id as tx_hash,
            block_id as block_number,
            index,
            tx_from as from_address,
            tx_to as to_address,
            tx_from in (
                select distinct (premint_address)
                from fact_solana_stablecoin_premint_addresses
            ) as is_mint,
            tx_to in (
                select distinct (premint_address)
                from fact_solana_stablecoin_premint_addresses
            ) as is_burn,
            coalesce(amount, 0) as amount,
            case
                when is_mint then amount when is_burn then -1 * amount else 0
            end as inflow,
            case
                -- Prior to 2023, volumes data not high fidelity enough to report. 
                -- Continuing to do analysis on this data. 
                when
                    not is_mint
                    and not is_burn
                    and trunc(block_timestamp, 'day') > '2022-12-31'
                then amount
                else 0
            end as transfer_volume,
            mint as contract_address,
            fact_{{ chain }}_stablecoin_contracts.symbol
        from solana_flipside.core.fact_transfers
        join
            fact_{{ chain }}_stablecoin_contracts
            on lower(solana_flipside.core.fact_transfers.mint)
            = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
        where
            mint
            in (select distinct contract_address from fact_solana_stablecoin_contracts)
    {% elif chain in ("near") %}
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            block_id as block_number,
            tx_hash,
            fact_token_transfers_id as index,
            from_address,
            to_address,
            from_address = 'system' or from_address is null as is_mint,
            to_address = 'system' or to_address is null as is_burn,
            amount_raw_precise / 1E24 as amount,
            case
                when is_mint then 1 * amount
                when is_burn then -1 * amount
                else 0
            end as inflow,
            case
                when not is_burn and not is_burn then amount else 0
            end as transfer_volume,
            t1.contract_address,
            fact_{{ chain }}_stablecoin_contracts.symbol
        from near_flipside.core.ez_token_transfers t1
        join
            fact_{{ chain }}_stablecoin_contracts
            on lower(t1.contract_address)
            = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
        where
            lower(t1.contract_address) in (
                select lower(contract_address)
                from fact_{{ chain }}_stablecoin_contracts
            )
            and transfer_type = 'nep141'
    {% else %}
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            block_number,
            event_index as index,
            {% if chain in ("celo") %}
                transaction_hash as tx_hash,
            {% else %}
                tx_hash,
            {% endif %}
            -- Notably, we do NOT use the Mint / Burn events here because the
            -- basic IERC20 interface does not require them to be implemented
            coalesce(
                decoded_log:from,
                -- DAI on ETH Mainnet does not follow the IERC20 interface
                decoded_log:src
            ) as from_address,
            coalesce(decoded_log:to, decoded_log:dst) as to_address,
            from_address = '0x0000000000000000000000000000000000000000'
            or event_name = 'Issue' as is_mint,
            to_address = '0x0000000000000000000000000000000000000000'
            or event_name = 'Redeem' as is_burn,
            coalesce(
                decoded_log:value::float / pow(10, num_decimals),
                decoded_log:wad::float / pow(10, num_decimals),
                -- USDT on ETH does not follow the IERC20 interface
                decoded_log:amount::float / pow(10, num_decimals),
                0
            ) as amount,
            case
                when is_mint then amount when is_burn then -1 * amount else 0
            end as inflow,
            case
                when not is_mint and not is_burn then amount else 0
            end as transfer_volume,
            t1.contract_address,
            fact_{{ chain }}_stablecoin_contracts.symbol
        {% if chain in ("celo")%} 
            from {{ref("fact_" ~ chain ~ "_decoded_events")}} t1 
        {% else %} 
            from {{ chain }}_flipside.core.ez_decoded_event_logs t1
        {% endif %}
        join
            fact_{{ chain }}_stablecoin_contracts
            on lower(t1.contract_address)
            = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
        where
            lower(t1.contract_address) in (
                select lower(contract_address)
                from fact_{{ chain }}_stablecoin_contracts
            )
            -- DO NOT include mint / burn events here - they will be duped
            and event_name in ('Transfer', 'Issue', 'Redeem')
        {% if chain in ("celo") %}
            and tx_status = 1
        {% else %}
            and tx_status = 'SUCCESS'
        {% endif %}
    {% endif %}
    {% if is_incremental() %} 
        and block_timestamp >= (
            select dateadd('day', -3, max(block_timestamp))
            from {{ this }}
        )
    {% endif %}
{% endmacro %}
