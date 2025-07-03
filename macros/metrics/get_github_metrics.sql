{% macro get_github_metrics(ecosystem) %}
    select
        ecosystem_name,
        commits_core.date,
        commits_core.val as weekly_commits_core_ecosystem,
        commits_sub.val as weekly_commits_sub_ecosystem,
        devs_core.val as weekly_developers_core_ecosystem,
        devs_sub.val as weekly_developers_sub_ecosystem
    from pc_dbt_db.prod.core_weeklycommitscoreecosystemwithoutforks as commits_core
    left join
        pc_dbt_db.prod.core_weeklycommitssubecosystemswithoutforks as commits_sub
        on commits_core.ecosystem_id = commits_sub.ecosystem_id
        and commits_core.date = commits_sub.date
    left join
        pc_dbt_db.prod.core_weeklydevscoreecosystemwithoutforks as devs_core
        on commits_core.ecosystem_id = devs_core.ecosystem_id
        and commits_core.date = devs_core.date
    left join
        pc_dbt_db.prod.core_weeklydevssubecosystemswithoutforks as devs_sub
        on commits_core.ecosystem_id = devs_sub.ecosystem_id
        and commits_core.date = devs_sub.date
    left join
        pc_dbt_db.prod.core_ecosystems as ecosystems
        on commits_core.ecosystem_id = ecosystems.id
    where 
        lower(ecosystem_name) = lower('{{ ecosystem }}')
        AND commits_core.date <= {{ latest_developer_data_date() }}
    order by date desc
{% endmacro %}
