{% macro get_wallet_stablecoin_metrics(chain) %}
    with
        stablecoin_transfers as ({{ agg_chain_stablecoin_transfers(chain) }}),
        -- stablecoin data
        generic_stablecoin_data as (
            select
                from_address as address,
                avg(amount) as avg_stablecoin_send,
                mode(to_address) as top_stablecoin_to_address,
                count(*) as number_of_stablecoin_transfers_txns,
                count(distinct to_address) as unique_count_to_address,
                min(block_timestamp) as first_stablecoin_transfer_date,
                max(block_timestamp) as latest_stablecoin_transfer_date,
                min_by(to_address, block_timestamp) as first_stablecoin_to_address
            from stablecoin_transfers
            group by from_address
        ),
        generic_stablecoin_received as (
            select
                to_address as address,
                avg(amount) as avg_stablecoin_received,
                mode(from_address) as top_stablecoin_from_address,
                count(*) as number_of_stablecoin_received_txns,
                count(distinct from_address) as unique_count_from_address,
                min(block_timestamp) as first_stablecoin_received_date,
                max(block_timestamp) as latest_stablecoin_received_date,
                min_by(from_address, block_timestamp) as first_stablecoin_from_address
            from stablecoin_transfers
            group by to_address
        )
    select
        stablecoin.address,
        first_stablecoin_to_address,
        first_stablecoin_from_address,
        avg_stablecoin_send,
        avg_stablecoin_received,
        top_stablecoin_to_address,
        top_stablecoin_from_address,
        number_of_stablecoin_transfers_txns,
        number_of_stablecoin_received_txns,
        unique_count_to_address,
        unique_count_from_address,
        first_stablecoin_transfer_date,
        latest_stablecoin_transfer_date,
        first_stablecoin_received_date,
        latest_stablecoin_received_date
    from generic_stablecoin_data as stablecoin
    full join
        generic_stablecoin_received
        on stablecoin.address = generic_stablecoin_received.address
{% endmacro %}
