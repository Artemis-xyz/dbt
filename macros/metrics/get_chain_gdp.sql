{% macro get_chain_gdp(chain) %}

WITH gdp_components AS (
    WITH fees_volume_and_emissions AS (
        SELECT
            date
            {% if chain == "ethereum" %}
                , COALESCE(fees, 0) AS fees
                , COALESCE(settlement_volume, 0) AS settlement_volume
                , COALESCE(blob_fees, 0) AS blob_fees
                , COALESCE(priority_fee_usd, 0) AS priority_fees
                , COALESCE(gross_emissions, 0) AS gross_emissions
            {% elif chain == "avalanche" %}
                , COALESCE(chain_fees, 0) AS rev
                , COALESCE(gross_emissions, 0) AS gross_emissions
                , COALESCE(settlement_volume, 0) AS settlement_volume
            {% elif chain == "solana" %}
                , COALESCE(rev, 0) AS rev 
                , COALESCE(settlement_volume, 0) AS settlement_volume
                , COALESCE(gross_emissions, 0) AS gross_emissions 
            {% elif chain == "arbitrum" %}
                , COALESCE(revenue, 0) AS rev
                , COALESCE(settlement_volume, 0) AS settlement_volume
                , NULL AS gross_emissions
            {% elif chain == "optimism" %}
                , COALESCE(revenue, 0) AS rev
                , COALESCE(settlement_volume, 0) AS settlement_volume
                , NULL AS gross_emissions
            {% endif %}
        FROM {{ ref('ez_' ~ chain ~ '_metrics') }}
    )

    -- need to subtract solana dex fees so I'm not double counting those in dex volumes above
    , protocol_revenue_data AS (
        SELECT 
            DATE_TRUNC(DAY, date) AS date 
            , SUM(COALESCE(fees, 0)) AS protocol_revenue
        FROM {{ ref("ez_protocol_datahub_by_chain") }}
        WHERE chain = '{{ chain }}'
        GROUP BY 1
    )

    SELECT 
        f.date 
        , p.protocol_revenue
        {% if chain == "ethereum" %}
            , f.gross_emissions
            , f.settlement_volume
            , f.fees + f.blob_fees + f.priority_fees AS rev
        {% elif chain == "avalanche" %}
            , f.gross_emissions
            , f.settlement_volume
            , f.rev
        {% elif chain == "solana" %}
            , f.gross_emissions
            , f.rev
            , f.settlement_volume
        {% elif chain == "arbitrum" %}
            , f.gross_emissions
            , f.rev
            , f.settlement_volume
        {% elif chain == "optimism" %}
            , f.gross_emissions
            , f.rev
            , f.settlement_volume
        {% endif %}
    FROM fees_volume_and_emissions f 
    LEFT JOIN protocol_revenue_data p
        ON f.date = p.date
)

SELECT 
    date
    , SUM(COALESCE(rev, 0)) AS rev
    , SUM(COALESCE(settlement_volume, 0)) AS settlement_volume
    , SUM(COALESCE(gross_emissions, 0)) AS gross_emissions
    , SUM(COALESCE(protocol_revenue, 0)) AS protocol_revenue
    , SUM(COALESCE(rev, 0)) + SUM(COALESCE(settlement_volume, 0)) + SUM(COALESCE(gross_emissions, 0)) + SUM(COALESCE(protocol_revenue, 0)) AS gdp
FROM gdp_components
GROUP BY 1
{% endmacro %}