{% macro daily_erc20_total_supply(token_address, decimals, chain) %}
    select date, sum(amount) over (order by date asc) as total_supply
    from
        (
            select date, sum(amount) amount
            from
                (
                    select
                        trunc(block_timestamp, 'day') as date,
                        case
                            when event_name = 'Burn' or event_name = 'TokensBurned'
                            then
                                -1 * decoded_log:amount::float / pow(10, {{ decimals }})
                            else decoded_log:amount::float / pow(10, {{ decimals }})
                        end as amount
                    from {{ chain }}_flipside.core.ez_decoded_event_logs
                    where
                        lower(contract_address) = lower('{{ token_address }}')
                        and event_name
                        in ('Mint', 'Burn', 'TokensMinted', 'TokensBurned')
                        and tx_succeeded = TRUE
                )
            group by date
        )
    order by 1 asc
{% endmacro %}
