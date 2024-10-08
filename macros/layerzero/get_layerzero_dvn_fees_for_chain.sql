{% macro get_layerzero_dvn_fees_for_chain(chain) %}

WITH fee_exploded AS (
        SELECT
            date(block_timestamp) AS date,
            value::NUMBER AS fee
        FROM
            {{chain}}_flipside.core.ez_decoded_event_logs,
            LATERAL FLATTEN(input => decoded_log['fees']) 
        WHERE
            event_name = 'DVNFeePaid'
        {% if is_incremental() %}
            and date(block_timestamp) >= (
                select dateadd('day', -1, max(date))
                from {{ this }}
                )
        {% endif %}
        ),
        total_fees_native AS (
            SELECT
                date,
                SUM(fee) / 1e18 AS total_fees_native
            FROM
                fee_exploded
            GROUP BY
                date
        ), 
        prices AS (
            SELECT
                date(hour) AS date,
                avg(price) as price
            FROM
                {{chain}}_flipside.price.ez_prices_hourly
            WHERE 
                is_native = True
                {% if chain == 'gnosis' %}
                    AND symbol = 'XDAI'
                {% endif %}
            GROUP BY 1
        )
        SELECT
            t.date,
            t.total_fees_native,
            p.price,
            t.total_fees_native * p.price AS total_fees_usd,
            '{{chain}}' as chain  
        FROM
            total_fees_native t
        INNER JOIN
          prices p
        ON
            t.date = p.date

{% endmacro %}