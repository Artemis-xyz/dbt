{% macro get_chain_gdp(chain) %}

WITH gdp_components AS (
    WITH fees_volume_and_emissions AS (
        SELECT
            date
            {% if chain == "ethereum" %}
                , COALESCE(fees, 0) AS fees
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume
                , COALESCE(blob_fees, 0) AS blob_fees
                , COALESCE(priority_fee_usd, 0) AS priority_fees
            {% elif chain == "avalanche" %}
                , COALESCE(chain_fees, 0) AS rev
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume
            {% elif chain == "solana" %}
                , COALESCE(rev, 0) AS rev 
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume 
            {% elif chain == "arbitrum" %}
                , COALESCE(revenue, 0) AS rev
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume
            {% elif chain == "optimism" %}
                , COALESCE(revenue, 0) AS rev
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume
            {% elif chain == "near" %}
                , COALESCE(blob_fees, 0) AS blob_fees
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume
                , NULL AS nft_trading_volume
                , COALESCE(chain_fees, 0) AS chain_fees
            {% elif chain == 'tron' %}
                , COALESCE(chain_fees, 0) AS rev
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume
                , NULL AS nft_trading_volume
            {% elif chain == 'polygon' %}
                , COALESCE(chain_fees, 0) AS chain_fees
                , COALESCE(l1_data_cost, 0) AS l1_data_cost
                , COALESCE(nft_trading_volume, 0) AS nft_trading_volume
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , COALESCE(p2p_native_transfer_volume, 0) AS p2p_native_transfer_volume
                , COALESCE(p2p_token_transfer_volume, 0) AS p2p_token_transfer_volume
            {% elif chain == 'sui' %}
                , COALESCE(chain_fees, 0) AS rev
                , NULL AS nft_trading_volume
                , COALESCE(dex_volumes, 0) AS dex_volumes
                , COALESCE(p2p_stablecoin_transfer_volume, 0) AS p2p_stablecoin_transfer_volume
                , NULL AS p2p_native_transfer_volume
                , NULL AS p2p_token_transfer_volume
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
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.fees + f.blob_fees + f.priority_fees AS rev
        {% elif chain == "avalanche" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.rev
        {% elif chain == "solana" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.rev
        {% elif chain == "arbitrum" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.rev
        {% elif chain == "optimism" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.rev
        {% elif chain == "near" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.chain_fees + f.blob_fees AS rev
            , f.dex_volumes AS settlement_volume
        {% elif chain == "tron" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.rev
        {% elif chain == "polygon" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.chain_fees - f.l1_data_cost AS rev
        {% elif chain == "sui" %}
            , f.nft_trading_volume
            , f.dex_volumes
            , f.p2p_stablecoin_transfer_volume
            , f.p2p_native_transfer_volume
            , f.p2p_token_transfer_volume
            , f.rev
        {% endif %}
    FROM fees_volume_and_emissions f 
    LEFT JOIN protocol_revenue_data p
        ON f.date = p.date
)

SELECT 
    date
    , SUM(COALESCE(rev, 0)) AS rev
    , SUM(COALESCE(nft_trading_volume, 0)) AS nft_trading_volume
    , SUM(COALESCE(dex_volumes, 0)) AS dex_volumes
    , SUM(COALESCE(p2p_stablecoin_transfer_volume, 0)) AS p2p_stablecoin_transfer_volume
    , SUM(COALESCE(p2p_token_transfer_volume, 0)) AS p2p_token_transfer_volume
    , SUM(COALESCE(p2p_native_transfer_volume, 0)) AS p2p_native_transfer_volume
    , SUM(COALESCE(protocol_revenue, 0)) AS protocol_revenue
    , SUM(COALESCE(rev, 0)) + SUM(COALESCE(dex_volumes, 0)) + SUM(COALESCE(protocol_revenue, 0)) + SUM(COALESCE(p2p_stablecoin_transfer_volume, 0)) + SUM(COALESCE(nft_trading_volume, 0)) + SUM(COALESCE(p2p_token_transfer_volume, 0)) + SUM(COALESCE(p2p_native_transfer_volume, 0)) AS gdp
FROM gdp_components
GROUP BY 1
{% endmacro %}