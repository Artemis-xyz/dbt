{% macro get_layerzero_executor_fees_for_chain(chain) %}

    WITH 
        fee_exploded AS (
            SELECT 
                date(block_timestamp) as date,
                SUM(PC_DBT_DB.PROD.HEX_TO_INT(substr(data, 67, 64))::number / 1e18) as eth_amount
            FROM {{chain}}_flipside.core.fact_event_logs
            WHERE topics[0] = '0x61ed099e74a97a1d7f8bb0952a88ca8b7b8ebd00c126ea04671f92a81213318a'
            GROUP BY date(block_timestamp)
        ),
        prices AS (
            SELECT
                date(hour) AS date,
                avg(price) as price
            FROM
                {{chain}}_flipside.price.ez_prices_hourly
            WHERE 
                is_native = True
            GROUP BY 1
        )
        SELECT
            f.date,
            f.eth_amount,
            p.price,
            f.eth_amount * p.price AS total_fees_usd,
            '{{chain}}' as chain  
        FROM
            fee_exploded f
        INNER JOIN
          prices p
        ON
            f.date = p.date

{% endmacro %}