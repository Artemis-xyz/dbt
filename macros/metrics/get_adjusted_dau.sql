{% macro get_adjusted_dau(chain) %}
    WITH txs AS (
        SELECT 
            t.raw_date
            , t.from_address 
            , w.number_of_apps_used
            , w.total_txns
            , w.distinct_to_address
            , w.number_of_days_active
            , w.first_native_received
            , w.funded_by_wallet_seeder_date
            -- rolling 30d balances
        FROM {{ ref("fact_" ~ chain ~ "_transactions_v2") }} t 
        LEFT JOIN {{ ref("dim_" ~ chain ~ "_wallet_v2") }} w
            ON t.from_address = w.address 
        {% if is_incremental() %}
            where t.raw_date >= (select max(raw_date) from {{ this }})
        {% endif %}
    )

    select 
        raw_date as date
        , count(distinct(from_address)) as adj_daus
    from txs 
    where 1=1
        and distinct_to_address >= 2 -- minimal diversity of interactions
        and number_of_days_active >= 3 -- minimal sustained activity
        and funded_by_wallet_seeder_date is null -- not funded by wallet seeder
        and first_native_received < raw_date - interval '7 days' -- minimum wallet age determined by first funding
    group by 1

{% endmacro %}
