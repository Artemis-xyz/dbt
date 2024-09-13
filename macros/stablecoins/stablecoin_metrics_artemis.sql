{% macro stablecoin_metrics_artemis(chain) %}
with
    stablecoin_transfers as (
        select 
            block_timestamp
            , tx_hash
            , from_address
            , contract_address
            , symbol
            , transfer_volume
            , to_address
        {% if chain in ("ton") %}
            from {{ ref("ez_" ~ chain ~ "_stablecoin_transfers")}} t
        {% else %}
            from {{ ref("fact_" ~ chain ~ "_stablecoin_transfers")}}
        {% endif %}
        {% if is_incremental() %} 
            where block_timestamp >= (
                select dateadd('day', -3, max(date))
                from {{ this }}
            )
        {% endif %} 
    ),
    filtered_contracts as (
        select * from {{ ref("dim_contracts_gold")}} where chain = '{{ chain }}'
    ),
    artemis_mev_filtered as (
        select
            st.*
            , coalesce(dl.app,'other') as from_app
            , coalesce(dlt.app,'other') as to_app
            , coalesce(dl.sub_category,'other') as from_category
            , coalesce(dlt.sub_category,'other') as to_category
        from stablecoin_transfers st
        left join filtered_contracts dl on lower(st.from_address) = lower(dl.address)
        left join filtered_contracts dlt on lower(st.to_address) = lower(dlt.address)
        where lower(from_app) != 'mev' and lower(to_app) != 'mev'
    ),
    artemis_cex_filters as (
        select distinct tx_hash
        from artemis_mev_filtered
        where from_app = to_app
            and lower(from_category) in ('cex', 'market maker') 
    ),
    artemis_ranked_transfer_filter as (
        select 
            artemis_mev_filtered.*,
            row_number() over (partition by tx_hash order by transfer_volume desc) AS rn
        from artemis_mev_filtered
        where tx_hash not in (select tx_hash from artemis_cex_filters)
    ),
    artemis_max_transfer_filter as (
        select 
            block_timestamp
            , tx_hash
            , contract_address
            , symbol
            , from_address
            , to_address
            , transfer_volume as stablecoin_transfer_volume
        from artemis_ranked_transfer_filter
        where rn = 1
    ),
    stablecoin_metrics as (
        select
            block_timestamp::date as date
            , from_address
            , contract_address
            , symbol
            , sum(
                case
                    when from_address is not null
                    then 1
                    else 0
                end
            ) as stablecoin_daily_txns
            , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
        from artemis_max_transfer_filter
        group by 1, 2, 3, 4
    )
    , results_dollar_denom as (
        select
            stablecoin_metrics.date
            , stablecoin_metrics.contract_address
            , stablecoin_metrics.symbol
            , from_address
            , stablecoin_transfer_volume * coalesce(
                d.shifted_token_price_usd, 
                case 
                    when c.coingecko_id = 'euro-coin' then ({{ avg_l7d_coingecko_price('euro-coin') }})
                    when c.coingecko_id = 'celo-euro' then ({{ avg_l7d_coingecko_price('celo-euro') }})
                    when c.coingecko_id = 'celo-real-creal' then ({{ avg_l7d_coingecko_price('celo-real-creal') }})
                    when c.coingecko_id = 'celo-kenyan-shilling' then ({{ avg_l7d_coingecko_price('celo-kenyan-shilling') }})
                    else 1
                end
            ) as stablecoin_transfer_volume
            , stablecoin_daily_txns
        from stablecoin_metrics
        left join {{ ref( "fact_" ~ chain ~ "_stablecoin_contracts") }} c
            on lower(stablecoin_metrics.contract_address) = lower(c.contract_address)
        left join {{ ref( "fact_coingecko_token_date_adjusted_gold") }} d
            on lower(c.coingecko_id) = lower(d.coingecko_id)
            and stablecoin_metrics.date = d.date::date
    )
    select
        date
        , from_address
        , contract_address
        , symbol
        , stablecoin_transfer_volume
        , stablecoin_daily_txns
        , '{{ chain }}' as chain
        , date || '-' || from_address || '-' || contract_address as unique_id
    from results_dollar_denom
    where date < to_date(sysdate())
    {% if is_incremental() %} 
        and date >= (
            select dateadd('day', -3, max(date))
            from {{ this }}
        )
    {% endif %} 
{% endmacro %}