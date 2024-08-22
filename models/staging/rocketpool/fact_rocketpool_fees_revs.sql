{{ config(materialized="table") }}

WITH joined_data AS (
    SELECT
        COALESCE(br.date, er.date, sp.date) AS date
        , br.cl_users_eth
        , br.cl_users_usd
        , br.cl_nodes_eth
        , br.cl_nodes_usd
        , er.el_fees_to_users_eth
        , er.el_fees_to_users_usd
        , er.el_fees_to_nodes_eth
        , er.el_fees_to_nodes_usd
        , sp.el_fees_smoothingpool_realized_users_eth
        , sp.el_fees_smoothingpool_realized_users_usd
        , sp.el_fees_smoothingpool_realized_stakers_eth
        , sp.el_fees_smoothingpool_realized_stakers_usd
        , sp.el_fees_smoothingpool_accrued_eth
        , sp.el_fees_smoothingpool_accrued_usd
    FROM {{ ref('fact_rocketpool_block_rewards') }} br
    FULL OUTER JOIN {{ ref('fact_rocketpool_el_rewards') }} er
        ON br.date = er.date
    FULL OUTER JOIN {{ ref('fact_rocketpool_smoothing_pool_fees') }} sp
        ON COALESCE(br.date, er.date) = sp.date
)
SELECT
    date
    , COALESCE(cl_users_eth, 0)
        + COALESCE(el_fees_to_users_eth, 0)
        + COALESCE(el_fees_smoothingpool_realized_users_eth, 0) AS total_user_rewards_eth
    , COALESCE(cl_users_usd, 0)
        + COALESCE(el_fees_to_users_usd, 0)
        + COALESCE(el_fees_smoothingpool_realized_users_usd, 0) AS total_user_rewards_usd
    , COALESCE(cl_nodes_eth, 0)
        + COALESCE(el_fees_to_nodes_eth, 0)
        + COALESCE(el_fees_smoothingpool_realized_stakers_eth, 0) AS total_node_rewards_eth
    , COALESCE(cl_nodes_usd, 0)
        + COALESCE(el_fees_to_nodes_usd, 0)
        + COALESCE(el_fees_smoothingpool_realized_stakers_usd, 0) AS total_node_rewards_usd
    ,  COALESCE(cl_users_eth,0)
        + COALESCE(cl_nodes_eth,0) as cl_rewards_eth
    , COALESCE(cl_users_usd,0)
        + COALESCE(cl_nodes_usd,0) as cl_rewards_usd
    ,  COALESCE(el_fees_to_users_eth,0)
        + COALESCE(el_fees_to_nodes_eth,0)
        + COALESCE(el_fees_smoothingpool_realized_users_eth,0)
        + COALESCE(el_fees_smoothingpool_realized_stakers_eth,0) as el_rewards_eth
    , COALESCE(el_fees_to_users_usd,0)
        + COALESCE(el_fees_to_nodes_usd,0)
        + COALESCE(el_fees_smoothingpool_realized_users_usd,0)
        + COALESCE(el_fees_smoothingpool_realized_stakers_usd,0) as el_rewards_usd
FROM joined_data