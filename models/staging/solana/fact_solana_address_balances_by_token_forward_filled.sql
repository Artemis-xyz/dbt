{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_XLG"
    )
}}

{% set token_addresses = var('token_addresses_list', []) %}


WITH address_balances AS (
    SELECT
        address,
        CASE
            WHEN contract_address = 'native_token' THEN 'solana:5eykt4usfv8p8njdtrepy1vzqkqzkvdp:native'
            ELSE contract_address
        END AS contract_address,
        block_timestamp,
        amount AS balance_raw,
        amount AS balance_native,
        decimals
    FROM {{ ref("fact_solana_address_balances_by_token") }}
    WHERE block_timestamp < to_date(sysdate())
    {% if is_incremental() %}
        AND block_timestamp > dateadd(day, -3, to_date(sysdate()))
    {% endif %}
    {% if token_addresses | length > 0 %}
        AND a.address IN (
            {{ "'" ~ token_addresses | join("','") ~ "'" }}
        )
    {% endif %}
),
{% if is_incremental() %}
    --Get the most recent data in the existing table
    stale_balances as (
        select 
            date as block_timestamp
            , t.contract_address
            , t.address
            , t.balance_raw
            , t.balance_native
        from {{ this }} t
        where date = (select dateadd('day', -3, max(date)) from {{ this }})
    ),
{% endif %}
heal_balance_table as (
    -- address_balances and stale_address_balances do not over lap
    -- address_balances select every row greater than the most recent date in the table
    -- stale_address_balances selects the most recent date in the table
    select
        block_timestamp
        , contract_address
        , address
        , balance_raw
        , balance_native
    from address_balances
    {% if is_incremental() %}
        union
        select 
            block_timestamp
            , contract_address
            , address
            , balance_raw
            , balance_native
        from stale_balances
    {% endif %}
), 
balances as (
    select 
        block_timestamp::date as date
        , contract_address
        , address
        , balance_raw
        , balance_native
    from (
        select 
            block_timestamp
            , contract_address
            , address
            , balance_raw
            , balance_native
            , row_number() over (partition by block_timestamp::date, contract_address, address order by block_timestamp desc) AS rn
        from heal_balance_table
    )
    where rn = 1
), 
date_spine AS (
    SELECT
        DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY NULL) - 1, DATE '2015-01-01') AS date
    FROM TABLE(GENERATOR(
        ROWCOUNT => DATEDIFF(DAY, DATE '2015-01-01', to_date(sysdate()) - 1)
    ))
),
date_range AS (
    SELECT 
        ds.date,
        min_dates.contract_address,
        min_dates.address
    FROM (
        SELECT 
            contract_address,
            address,
            MIN(block_timestamp)::DATE AS start_date
        FROM heal_balance_table
        GROUP BY contract_address, address
    ) min_dates
    JOIN date_spine ds
        ON ds.date BETWEEN p.start_date AND to_date(sysdate()) - 1;
),
historical_supply_by_address_balances as (
    select
        date
        , address
        , contract_address
        , coalesce(
            balance_raw, 
            LAST_VALUE(balances.balance_raw ignore nulls) over (
                partition by contract_address, address
                order by date
                rows between unbounded preceding and current row
            ) 
        )  as balance_raw
        , coalesce(
            balance_native, 
            LAST_VALUE(balances.balance_native ignore nulls) over (
                partition by contract_address, address
                order by date
                rows between unbounded preceding and current row
            ) 
        )  as balance_native
    from date_range
    left join balances using (date, contract_address, address)
), 
prices as ({{ get_multiple_coingecko_price_with_latest(chain) }} ), 
address_balances_with_prices as (
    select
        date
        , contract_address
        , symbol
        , address
        , price
        , balance_raw
        , balance_native
        , balance_native * price as balance
    from historical_supply_by_address_balances
    left join prices using (date, contract_address)
)
select 
    date
    , contract_address
    , symbol
    , address
    , balance_raw
    , balance_native
    , price
    , balance
from address_balances_with_prices


