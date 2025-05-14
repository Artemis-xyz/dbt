{% macro get_wallet_fundamental_metrics_v2(chain) %}
    -- get all application, category, sub category they've every interacted with
    with
        from_address_labeled_data as (
            select
                from_address as address,
                array_unique_agg(app) as app_used,
                array_size(app_used::VARIANT) as number_of_apps_used,
                array_unique_agg(category) as category_used,
                array_size(category_used::VARIANT) as number_of_categories_used,
                sum(gas_usd) as total_gas_spent_usd,
                sum(tx_fee) as total_gas_spent_native,
                count(*) as total_txns,
                count(distinct contract_address) as distinct_to_address,
                max(block_timestamp) as latest_transaction_timestamp,
                min(block_timestamp) as first_transaction_timestamp,
                count(distinct(date_trunc('day', block_timestamp))) as number_of_days_active,
                mode(app) as top_app,
                mode(contract_address) as top_to_address
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            group by from_address
        ),
        -- First occurance of native token transfer
        first_native_transfer as (
            select
                from_address as address,
                min_by(to_address, block_timestamp) as first_native_transfer
            from {{ chain }}_flipside.core.fact_transactions
            where value > 0
            group by from_address
        ),

        first_native_received as (
            select
                recipient as address,
                block_timestamp as first_native_received
            from PC_DBT_DB.PROD.FACT_ARBITRUM_FIRST_FUNDING
        ),

        first_bridge_used as (
            select
                from_address as address,
                min_by(app, block_timestamp) as first_bridge_used
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            where category = 'Bridge'
            group by from_address
        ),

        first_app as (
            select from_address as address, min_by(app, block_timestamp) as first_app
            from {{ ref("fact_" ~ chain ~ "_transactions_v2") }}
            where app is not null
            group by from_address
        ),

        -- get there most received level metrics
        to_address_transaction_data as (
            select
                to_address as address,
                mode(from_address) top_from_address,
                min_by(from_address, block_timestamp) as first_from_address
            from {{ chain }}_flipside.core.fact_transactions
            group by to_address
        ),

        funded_by_wallet_seeder as (
            select
                to_address as address,
                block_timestamp as funded_by_wallet_seeder_date,
                tx_hash as funded_by_wallet_seeder_tx_hash 
            from PC_DBT_DB.PROD.FACT_ARBITRUM_WALLET_SEEDER_FUNDING_RECIPIENTS
        )

    select
        COALESCE(
            from_address.address, 
            first_app.address, 
            first_native_transfer.address, 
            first_native_received.address, 
            first_bridge_used.address,
            to_address_transaction_data.address,
            funded_by_wallet_seeder.address
        ) as address,
        app_used,
        number_of_apps_used,
        category_used,
        number_of_categories_used,
        total_gas_spent_usd,
        total_gas_spent_native,
        total_txns,
        distinct_to_address,
        latest_transaction_timestamp,
        first_transaction_timestamp,
        number_of_days_active,
        first_app,
        top_app,
        top_to_address,
        first_native_transfer,
        first_native_received,
        first_bridge_used,
        top_from_address,
        first_from_address,
        funded_by_wallet_seeder_date,
        funded_by_wallet_seeder_tx_hash
    from from_address_labeled_data as from_address
    full join first_app on from_address.address = first_app.address
    full join
        first_native_transfer on from_address.address = first_native_transfer.address
    full join
        first_native_received on from_address.address = first_native_received.address
    full join first_bridge_used on from_address.address = first_bridge_used.address
    full join
        to_address_transaction_data
        on from_address.address = to_address_transaction_data.address
    left join funded_by_wallet_seeder on from_address.address = funded_by_wallet_seeder.address
{% endmacro %}
