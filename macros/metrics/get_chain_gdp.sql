{% macro get_chain_gdp(chain) %}

WITH consumption_and_gov_expenditure AS (
    WITH gas_blob_fees_and_nft_volume AS (
        SELECT
            date
            {% if chain == "ethereum" %}
                , COALESCE(fees, 0) as gas
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume -- consumption 
                , COALESCE(blob_fees, 0) AS blob_fees
                , COALESCE(priority_fee_usd, 0) AS priority_fees
                , COALESCE(gross_emissions, 0) AS gross_emissions -- gov expenditures
            {% endif %}
            {% if chain == "solana" %}
                , COALESCE(rev, 0) as rev 
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume -- consumption 
                , COALESCE(gross_emissions, 0) AS gross_emissions -- gov expenditures
            {% endif %}
        FROM {{ ref('ez_' ~ chain ~ '_metrics') }}
    )

    -- need to add protocol revenue
    , protocol_revenue AS (
        SELECT 
            DATE_TRUNC(DAY, date) AS date 
            , SUM(ecosystem_revenue) AS ecosystem_revenue
        FROM {{ ref("ez_protocol_datahub_by_chain") }}
        WHERE chain = '{{ chain }}'
        GROUP BY 1
    )

    SELECT 
        g.date 
        , COALESCE(p.ecosystem_revenue, 0) AS protocol_revenue
        {% if chain == "ethereum" %}
            , g.gross_emissions
            , g.gas + g.nft_trading_volume + g.blob_fees + protocol_revenue + g.priority_fees as consumption
        {% endif %}
        {% if chain == "solana" %}
            , g.gross_emissions
            , g.rev + g.nft_trading_volume + protocol_revenue as consumption
        {% endif %}
    FROM gas_blob_fees_and_nft_volume g 
    LEFT JOIN protocol_revenue p
        ON g.date = p.date
)

-- net flows
, net_flows AS (
    WITH inflows AS (
        SELECT
            date 
            , SUM(amount_usd) AS inflows
        FROM {{ ref("agg_daily_bridge_flows_metrics") }}
        WHERE destination_chain = '{{ chain }}'
        GROUP BY 1
    )

    , outflows AS (
        SELECT
            date 
            , SUM(amount_usd) AS outflows
        FROM {{ ref("agg_daily_bridge_flows_metrics") }}
        WHERE source_chain = '{{ chain }}'
        GROUP BY 1
    )

    SELECT
        COALESCE(i.date, o.date) AS date 
        , i.inflows - o.outflows AS net_flows
    FROM inflows i 
    FULL OUTER JOIN outflows o
        ON i.date = o.date
)

, funding AS (
    WITH cleaned_and_flattened_funding_data AS (
        SELECT 
            *
            , STRTOK_TO_ARRAY(REPLACE(REPLACE(REPLACE(chains, '[', ''), ']', ''),  '''', ''), ',') AS cleaned_chains
            , v.value::VARCHAR AS cleaned_single_chain
        FROM PC_DBT_DB.PROD.DEFILLAMA_RAISES d,
        LATERAL FLATTEN (input => cleaned_chains) v
    )
    
    SELECT 
        date 
        , cleaned_single_chain
        , SUM(amount * 1000000) AS funding_amount -- need to multiply by 1M
    FROM  cleaned_and_flattened_funding_data
    WHERE LOWER(cleaned_single_chain) LIKE '%{{ chain }}%'
    GROUP BY 1, 2
)

, daily_gdp AS (
    SELECT 
        cg.date 
        , COALESCE(cg.consumption, 0) AS consumption -- consumption
        , COALESCE(cg.gross_emissions, 0) AS expenditures -- government expenditures
        , COALESCE(nf.net_flows, 0) AS exports -- net exports
        , COALESCE(f.funding_amount, 0) AS investment
        , consumption + expenditures + exports + investment AS chain_gdp
    FROM consumption_and_gov_expenditure cg
    LEFT JOIN net_flows nf  
        ON cg.date = nf.date
    LEFT JOIN funding f
        ON cg.date = f.date
)

SELECT 
    date 
    , SUM(consumption) AS consumption
    , SUM(expenditures) AS expenditures
    , SUM(exports) AS exports
    , SUM(investment) AS investment
    , SUM(chain_gdp) AS gdp 
FROM daily_gdp 
GROUP BY 1

{% endmacro %}