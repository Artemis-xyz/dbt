{% macro get_censored_block_metrics(chain) %}
    select
        date,
        chain,
        sum(
            case when censors = 'Censoring' then blocks_produced else 0 end
        ) as censored_blocks,
        sum(
            case when censors = 'Semi-Censoring' then blocks_produced else 0 end
        ) as semi_censored_blocks,
        sum(
            case when censors = 'Non-Censoring' then blocks_produced else 0 end
        ) as non_censored_blocks,
        sum(blocks_produced) as total_blocks_produced,
        (
            sum(case when censors = 'Censoring' then blocks_produced else 0 end)
            / sum(blocks_produced)::float
        )
        * 100 as percent_censored,
        (
            sum(case when censors = 'Semi-Censoring' then blocks_produced else 0 end)
            / sum(blocks_produced)::float
        )
        * 100 as percent_semi_censored,
        (
            sum(case when censors = 'Non-Censoring' then blocks_produced else 0 end)
            / sum(blocks_produced)::float
        )
        * 100 as percent_non_censored
    from {{ ref("fact_" ~ chain ~ "_block_producers_silver") }}
    group by date, chain
{% endmacro %}
