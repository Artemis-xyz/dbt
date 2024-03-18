{{ config(materialized="table") }}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_public_comps_data") }}
    ),
    latest_data as (
        select
            parse_json(
                replace(
                    replace(replace(replace(source_json, '%', ''), '>', ''), '<', ''),
                    '~',
                    ''
                )
            ) as data  -- there are random % signs in the data: strip it out
        from {{ source("PROD_LANDING", "raw_public_comps_data") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            -- flattened.value:"id"::INT AS id,
            flattened.value:"name"::string as name,
            flattened.value:"categories"::string as categories,
            flattened.value:"market_cap"::float as market_cap,
            flattened.value:"current_market_cap"::float as current_market_cap,
            flattened.value:"enterprise_value"::float as enterprise_value,
            flattened.value:"yoy_growth"::float as yoy_growth,
            flattened.value:"gross_margin"::float as gross_margin,
            flattened.value:"annual_run_rate"::float as annual_run_rate,
            flattened.value:"last_year_run_rate"::float as last_year_run_rate,
            flattened.value:"ltm_revenue"::float as ltm_revenue,
            -- flattened.value:"ev_over_run_rate"::FLOAT AS ev_over_run_rate,
            -- flattened.value:"ev_over_ltm_revenue"::FLOAT AS ev_over_ltm_revenue,
            -- flattened.value:"ev_over_ltm_free_cash_flow_percent"::FLOAT AS
            -- ev_over_ltm_free_cash_flow_percent,
            -- flattened.value:"ltm_operating_cash_flow_percent"::FLOAT AS
            -- ltm_operating_cash_flow_percent,
            -- flattened.value:"ev_over_ltm_free_cash_flow"::FLOAT AS
            -- ev_over_ltm_free_cash_flow,
            flattened.value:"free_cash_flow_margin"::float as free_cash_flow_margin,
            flattened.value:"ticker"::string as ticker,
            flattened.value:"share_price"::float as share_price,
            flattened.value:"net_dollar_retention"::float as net_dollar_retention,
            -- flattened.value:"ltm_free_cash_flow_percent"::FLOAT AS
            -- ltm_free_cash_flow_percent,
            -- flattened.value:"efficiency"::FLOAT AS efficiency,
            -- flattened.value:"form_url"::STRING AS form_url,
            -- flattened.value:"presentation_url"::STRING AS presentation_url,
            flattened.value:"magic_number"::float as magic_number,
            flattened.value:"cac_ratio"::float as cac_ratio,
            flattened.value:"payback_period"::float as payback_period,
            -- flattened.value:"ev_over_2019_estimates"::FLOAT AS
            -- ev_over_2019_estimates,
            -- flattened.value:"ev_over_2020_estimates"::FLOAT AS
            -- ev_over_2020_estimates,
            -- flattened.value:"ev_over_2020_revenue"::FLOAT AS ev_over_2020_revenue,
            -- flattened.value:"stripped_name"::STRING AS stripped_name,
            -- flattened.value:"research_development_margin"::FLOAT AS
            -- research_development_margin,
            -- flattened.value:"sales_marketing_margin"::FLOAT AS
            -- sales_marketing_margin,
            -- flattened.value:"general_admin_margin"::FLOAT AS general_admin_margin,
            -- flattened.value:"quarter_end"::DATE AS quarter_end,
            flattened.value:"annual_recurring_revenue"::float
            as annual_recurring_revenue,
            flattened.value:"arr_growth"::float as arr_growth,
            flattened.value:"quarter"::date as quarter,
            -- flattened.value:"growth_persistence"::FLOAT AS growth_persistence,
            -- flattened.value:"operating_income_percent"::FLOAT AS
            -- operating_income_percent,
            flattened.value:"run_rate_per_employee"::float as run_rate_per_employee,
            -- flattened.value:"last_year_quarter_growth"::FLOAT AS
            -- last_year_quarter_growth,
            -- flattened.value:"revenue_estimates_2020"::FLOAT AS
            -- revenue_estimates_2020,
            -- flattened.value:"revenue_2020"::FLOAT AS revenue_2020,
            -- flattened.value:"revenue_2019"::FLOAT AS revenue_2019,
            -- flattened.value:"share_price_percent_of_high"::FLOAT AS
            -- share_price_percent_of_high,
            -- flattened.value:"free_cash_flow_percent"::FLOAT AS
            -- free_cash_flow_percent,
            flattened.value:"current_magic_number"::float as current_magic_number,  -- new with Jon Ma
            -- flattened.value:"founding_year"::INT AS founding_year,
            -- flattened.value:"ipo_year"::INT AS ipo_year,
            -- flattened.value:"multiple_return_on_ipo"::FLOAT AS
            -- multiple_return_on_ipo,
            -- flattened.value:"implied_cac"::FLOAT AS implied_cac,
            -- flattened.value:"implied_five_year_ltv"::FLOAT AS implied_five_year_ltv,
            -- flattened.value:"implied_five_year_ltv_cac"::FLOAT AS
            -- implied_five_year_ltv_cac,
            flattened.value:"arr_per_customer"::float as arr_per_customer,  -- new with Jon Ma
            -- flattened.value:"ev_over_2021_estimates"::FLOAT AS
            -- ev_over_2021_estimates,
            -- flattened.value:"ltm_free_cash_flow_growth"::FLOAT AS
            -- ltm_free_cash_flow_growth,
            -- flattened.value:"ev_over_ntm_revenue"::FLOAT AS ev_over_ntm_revenue,
            -- flattened.value:"effective_cash_m"::FLOAT AS effective_cash_m,
            -- flattened.value:"net_cash_m"::FLOAT AS net_cash_m,
            flattened.value:"ltm_revenue_growth"::float as ltm_revenue_growth,
            -- flattened.value:"last_year_quarter_revenue"::FLOAT AS
            -- last_year_quarter_revenue,
            flattened.value:"revenue"::float as revenue,
            flattened.value:"free_cash_flow"::float as free_cash_flow,
            -- flattened.value:"ltm_free_cash_flow"::FLOAT AS ltm_free_cash_flow,
            -- flattened.value:"last_year_best_url"::STRING AS last_year_best_url,
            flattened.value:"ntm_revenue"::float as ntm_revenue,  -- new with Jon Ma
            flattened.value:"cash"::float as cash,
            -- flattened.value:"short_term_investments"::FLOAT AS
            -- short_term_investments,
            -- flattened.value:"revenue_estimates_2021"::FLOAT AS
            -- revenue_estimates_2021,
            -- flattened.value:"shares"::FLOAT AS shares,
            -- flattened.value:"effective_cash"::FLOAT AS effective_cash,
            -- flattened.value:"effective_debt"::FLOAT AS effective_debt,
            -- flattened.value:"long_term_debt"::FLOAT AS long_term_debt,
            -- flattened.value:"short_term_debt"::FLOAT AS short_term_debt,
            -- flattened.value:"convertible_note"::FLOAT AS convertible_note,
            flattened.value:"gross_profit"::float as gross_profit,
            -- flattened.value:"lytq_ltm_revenue"::FLOAT AS lytq_ltm_revenue,
            flattened.value:"previous_quarter_revenue"::float
            as previous_quarter_revenue,  -- new with Jon Ma
            flattened.value:"two_quarters_ago_revenue"::float
            as two_quarters_ago_revenue,  -- new with Jon Ma
            -- flattened.value:"three_quarters_ago_revenue"::FLOAT AS
            -- three_quarters_ago_revenue,
            -- flattened.value:"two_quarters_ago_best_url"::STRING AS
            -- two_quarters_ago_best_url,
            -- flattened.value:"three_quarters_ago_best_url"::STRING AS
            -- three_quarters_ago_best_url,
            -- flattened.value:"previous_quarter_best_url"::STRING AS
            -- previous_quarter_best_url,
            -- flattened.value:"previous_quarter_free_cash_flow"::FLOAT AS
            -- previous_quarter_free_cash_flow,
            -- flattened.value:"two_quarters_ago_free_cash_flow"::FLOAT AS
            -- two_quarters_ago_free_cash_flow,
            -- flattened.value:"three_quarters_ago_free_cash_flow"::FLOAT AS
            -- three_quarters_ago_free_cash_flow,
            -- flattened.value:"share_price_updated_at"::FLOAT AS
            -- share_price_updated_at,
            -- flattened.value:"cash_equivalents"::FLOAT AS cash_equivalents,
            -- flattened.value:"marketable_securities"::FLOAT AS marketable_securities,
            -- flattened.value:"other_debt"::FLOAT AS other_debt,
            -- flattened.value:"research_development"::FLOAT AS research_development,
            flattened.value:"sales_marketing"::float as sales_marketing,  -- new with Jon Ma
            -- flattened.value:"general_admin"::FLOAT AS general_admin,
            -- flattened.value:"us_revenue"::FLOAT AS us_revenue,
            -- flattened.value:"previous_quarter_current_magic_number"::FLOAT AS
            -- previous_quarter_current_magic_number,
            -- flattened.value:"two_quarters_ago_current_magic_number"::FLOAT AS
            -- two_quarters_ago_current_magic_number,
            -- flattened.value:"three_quarters_ago_current_magic_number"::FLOAT AS
            -- three_quarters_ago_current_magic_number,
            -- flattened.value:"previous_quarter_sales_marketing"::FLOAT AS
            -- previous_quarter_sales_marketing,
            -- flattened.value:"two_quarters_ago_sales_marketing"::FLOAT AS
            -- two_quarters_ago_sales_marketing,
            -- flattened.value:"three_quarters_ago_sales_marketing"::FLOAT AS
            -- three_quarters_ago_sales_marketing,
            -- flattened.value:"last_year_ticker_quarter_sales_marketing"::FLOAT AS
            -- last_year_ticker_quarter_sales_marketing,
            -- flattened.value:"new_revenue_annualized"::FLOAT AS
            -- new_revenue_annualized,
            -- flattened.value:"last_year_quarter_new_revenue_annualized"::FLOAT AS
            -- last_year_quarter_new_revenue_annualized,
            -- flattened.value:"two_quarters_ago_new_revenue_annualized"::FLOAT AS
            -- two_quarters_ago_new_revenue_annualized,
            -- flattened.value:"three_quarters_ago_new_revenue_annualized"::FLOAT AS
            -- three_quarters_ago_new_revenue_annualized,
            -- flattened.value:"previous_quarter_new_revenue_annualized"::FLOAT AS
            -- previous_quarter_new_revenue_annualized,
            -- flattened.value:"ev_over_2022_estimates"::FLOAT AS
            -- ev_over_2022_estimates,
            -- flattened.value:"ev_over_2023_estimates"::FLOAT AS
            -- ev_over_2023_estimates,
            -- flattened.value:"ntm_revenue_growth"::FLOAT AS ntm_revenue_growth,
            -- flattened.value:"estimates_growth_2020"::FLOAT AS estimates_growth_2020,
            -- flattened.value:"estimates_growth_2021"::FLOAT AS estimates_growth_2021,
            -- flattened.value:"estimates_growth_2022"::FLOAT AS estimates_growth_2022,
            -- flattened.value:"estimates_growth_2023"::FLOAT AS estimates_growth_2023,
            flattened.value:"new_annual_recurring_revenue"::float
            as new_annual_recurring_revenue,
            flattened.value:"arr_per_employee"::float as arr_per_employee,
            -- flattened.value:"annualized_opex_per_employee"::FLOAT AS
            -- annualized_opex_per_employee,
            -- flattened.value:"ev_over_arr"::FLOAT AS ev_over_arr,
            -- flattened.value:"ev_over_ntm_arr"::FLOAT AS ev_over_ntm_arr,
            -- flattened.value:"computed_opex"::FLOAT AS computed_opex,
            flattened.value:"employee_count"::float as employee_count,
            -- flattened.value:"arr_type"::STRING AS arr_type,
            flattened.value:"customers"::float as customers,  -- new with Jon Ma
            -- flattened.value:"is_ipo_quarter"::BOOLEAN AS is_ipo_quarter,
            -- flattened.value:"research_and_development_employee_count"::FLOAT AS
            -- research_and_development_employee_count,
            -- flattened.value:"sales_and_marketing_employee_count"::FLOAT AS
            -- sales_and_marketing_employee_count,
            -- flattened.value:"general_and_administrative_employee_count"::FLOAT AS
            -- general_and_administrative_employee_count,
            -- flattened.value:"people_employee_count"::FLOAT AS people_employee_count,
            -- flattened.value:"finance_employee_count"::FLOAT AS
            -- finance_employee_count,
            -- flattened.value:"support_employee_count"::FLOAT AS
            -- support_employee_count,
            -- flattened.value:"marketing_employee_count"::FLOAT AS
            -- marketing_employee_count,
            -- flattened.value:"ntm_revenue_updated"::DATE AS ntm_revenue_updated,
            flattened.value:"calendar_year"::int as calendar_year,
            flattened.value:"calendar_quarter"::int as calendar_quarter,
            -- flattened.value:"efficiency_operating"::FLOAT AS efficiency_operating,
            -- flattened.value:"best_url"::STRING AS best_url,
            -- flattened.value:"ev_over_2020_arr_estimates"::FLOAT AS
            -- ev_over_2020_arr_estimates,
            -- flattened.value:"efficiency_current_fcf"::FLOAT AS
            -- efficiency_current_fcf,
            -- flattened.value:"current_cac_ratio"::FLOAT AS current_cac_ratio,
            -- flattened.value:"cac_payback_period"::FLOAT AS cac_payback_period,
            -- flattened.value:"last_year_ticker_quarter_annual_recurring_revenue"::FLOAT AS last_year_ticker_quarter_annual_recurring_revenue,
            -- flattened.value:"ev_over_2022_gross_profit"::FLOAT AS
            -- ev_over_2022_gross_profit,
            -- flattened.value:"ev_over_ntm_gross_profit"::FLOAT AS
            -- ev_over_ntm_gross_profit,
            -- flattened.value:"ev_over_ntm_gross_profit_run_rate"::FLOAT AS
            -- ev_over_ntm_gross_profit_run_rate,
            -- flattened.value:"qoq_arr_growth"::FLOAT AS qoq_arr_growth,
            -- flattened.value:"consensus_revenue_mean"::FLOAT AS
            -- consensus_revenue_mean,
            -- flattened.value:"revenue_beat_mean"::FLOAT AS revenue_beat_mean,
            flattened.value:"ebitda"::float as ebitda,
            -- flattened.value:"ev_over_ltm_ebitda"::FLOAT AS ev_over_ltm_ebitda,
            -- flattened.value:"depreciation_amoritization"::FLOAT AS
            -- depreciation_amoritization,
            flattened.value:"operating_income"::float as operating_income,
            -- flattened.value:"ltm_operating_income"::FLOAT AS ltm_operating_income,
            -- flattened.value:"previous_quarter_annual_recurring_revenue"::FLOAT AS
            -- previous_quarter_annual_recurring_revenue,
            -- flattened.value:"cost_of_revenue"::FLOAT AS cost_of_revenue,
            flattened.value:"net_income"::float as net_income,
            -- flattened.value:"sbc_research_and_development"::FLOAT AS
            -- sbc_research_and_development,
            -- flattened.value:"sbc_sales_and_marketing"::FLOAT AS
            -- sbc_sales_and_marketing,
            -- flattened.value:"sbc_general_and_administrative"::FLOAT AS
            -- sbc_general_and_administrative,
            -- flattened.value:"sbc_cost_of_revenue"::FLOAT AS sbc_cost_of_revenue,
            -- flattened.value:"sbc_expenses"::FLOAT AS sbc_expenses,
            -- flattened.value:"adjusted_research_and_development"::FLOAT AS
            -- adjusted_research_and_development,
            -- flattened.value:"adjusted_sales_and_marketing"::FLOAT AS
            -- adjusted_sales_and_marketing,
            -- flattened.value:"adjusted_general_and_administrative"::FLOAT AS
            -- adjusted_general_and_administrative,
            -- flattened.value:"adjusted_cost_of_revenue"::FLOAT AS
            -- adjusted_cost_of_revenue,
            -- flattened.value:"adjusted_expenses"::FLOAT AS adjusted_expenses,
            -- flattened.value:"remaining_performance_obligation"::FLOAT AS
            -- remaining_performance_obligation,
            flattened.value:"ebitda_margin"::float as ebitda_margin,  -- new with Jon Ma
            flattened.value:"ltm_ebitda"::float as ltm_ebitda,  -- new with Jon Ma
            -- flattened.value:"outstanding_shares"::FLOAT AS outstanding_shares,
            -- flattened.value:"current_common_stock"::FLOAT AS current_common_stock,
            -- flattened.value:"changes_in_deferred_revenue"::FLOAT AS
            -- changes_in_deferred_revenue,
            -- flattened.value:"arr_growth_persistence"::FLOAT AS
            -- arr_growth_persistence,
            flattened.value:"yoy_customer_growth"::float as yoy_customer_growth,
            -- flattened.value:"international_revenue"::FLOAT AS international_revenue,
            flattened.value:"subscription_revenue"::float as subscription_revenue,  -- new with Jon Ma
            -- flattened.value:"services_revenue"::FLOAT AS services_revenue,
            -- flattened.value:"other_revenue"::FLOAT AS other_revenue,
            -- flattened.value:"revenue_guidance_low"::FLOAT AS revenue_guidance_low,
            -- flattened.value:"revenue_guidance_high"::FLOAT AS revenue_guidance_high,
            -- flattened.value:"free_cash_flow_run_rate"::FLOAT AS
            -- free_cash_flow_run_rate,
            -- flattened.value:"operating_cash_flow"::FLOAT AS operating_cash_flow,
            -- flattened.value:"purchases_of_property_and_equipment"::FLOAT AS
            -- purchases_of_property_and_equipment,
            -- flattened.value:"capitalized_internal_use_software"::FLOAT AS
            -- capitalized_internal_use_software,
            -- flattened.value:"capital_expenditure"::FLOAT AS capital_expenditure,
            -- flattened.value:"net_cash_used_in_provided_by_investing_activities"::FLOAT AS net_cash_used_in_provided_by_investing_activities,
            -- flattened.value:"net_cash_provided_by_financing_activities"::FLOAT AS
            -- net_cash_provided_by_financing_activities,
            -- flattened.value:"restricted_cash"::FLOAT AS restricted_cash,
            flattened.value:"employees"::float as employees,  -- new with Jon Ma
            flattened.value:"net_dollar_retention_percent"::float
            as net_dollar_retention_percent,  -- new with Jon Ma
            -- flattened.value:"spreadsheet_ndr"::FLOAT AS spreadsheet_ndr,
            flattened.value:"gross_merchandise_volume"::float
            as gross_merchandise_volume,  -- new with Jon Ma
            flattened.value:"total_payment_value"::float as total_payment_value,  -- new with Jon Ma
            -- flattened.value:"current_remaining_performance_obligation"::FLOAT AS
            -- current_remaining_performance_obligation,
            -- flattened.value:"previous_quarter_current_cac_ratio"::FLOAT AS
            -- previous_quarter_current_cac_ratio,
            -- flattened.value:"two_quarters_ago_current_cac_ratio"::FLOAT AS
            -- two_quarters_ago_current_cac_ratio,
            -- flattened.value:"three_quarters_ago_current_cac_ratio"::FLOAT AS
            -- three_quarters_ago_current_cac_ratio,
            -- flattened.value:"previous_quarter_gross_margin"::FLOAT AS
            -- previous_quarter_gross_margin,
            -- flattened.value:"two_quarters_ago_gross_margin"::FLOAT AS
            -- two_quarters_ago_gross_margin,
            -- flattened.value:"three_quarters_ago_gross_margin"::FLOAT AS
            -- three_quarters_ago_gross_margin,
            -- flattened.value:"ltm_ebitda_margin"::FLOAT AS ltm_ebitda_margin,
            -- flattened.value:"previous_quarter_operating_cash_flow"::FLOAT AS
            -- previous_quarter_operating_cash_flow,
            -- flattened.value:"two_quarters_ago_operating_cash_flow"::FLOAT AS
            -- two_quarters_ago_operating_cash_flow,
            -- flattened.value:"three_quarters_ago_operating_cash_flow"::FLOAT AS
            -- three_quarters_ago_operating_cash_flow,
            -- flattened.value:"previous_quarter_capital_expenditure"::FLOAT AS
            -- previous_quarter_capital_expenditure,
            -- flattened.value:"two_quarters_ago_capital_expenditure"::FLOAT AS
            -- two_quarters_ago_capital_expenditure,
            -- flattened.value:"three_quarters_ago_capital_expenditure"::FLOAT AS
            -- three_quarters_ago_capital_expenditure,
            -- flattened.value:"annualized_opex"::FLOAT AS annualized_opex,
            -- flattened.value:"total_opex"::FLOAT AS total_opex,
            -- flattened.value:"employee_report_quarter"::DATE AS
            -- employee_report_quarter,
            -- flattened.value:"last_earnings_date"::DATE AS last_earnings_date,
            -- flattened.value:"next_earnings_date"::DATE AS next_earnings_date,
            -- flattened.value:"revenue_estimates_2022"::FLOAT AS
            -- revenue_estimates_2022,
            flattened.value:"revenue_estimates_2023"::float as revenue_estimates_2023,  -- new with Jon Ma
            -- flattened.value:"ev_over_2021_revenue"::FLOAT AS ev_over_2021_revenue,
            -- flattened.value:"ev_over_2022_revenue"::FLOAT AS ev_over_2022_revenue,
            -- flattened.value:"ev_over_gross_merchandise_volume"::FLOAT AS
            -- ev_over_gross_merchandise_volume,
            flattened.value:"burn_multiple"::float as burn_multiple,  -- new with Jon Ma
            flattened.value:"ltm_burn_multiple"::float as ltm_burn_multiple,
            -- flattened.value:"revenue_2021"::FLOAT AS revenue_2021,  -- new with Jon
            -- Ma
            -- flattened.value:"revenue_2022"::FLOAT AS revenue_2022,  -- new with Jon
            -- Ma
            flattened.value:"gross_retention"::float as gross_retention,
            -- flattened.value:"price_change_pct_1d"::FLOAT AS price_change_pct_1d,
            -- flattened.value:"price_change_pct_7d"::FLOAT AS price_change_pct_7d,
            -- flattened.value:"price_change_pct_30d"::FLOAT AS price_change_pct_30d,
            -- flattened.value:"price_change_pct_60d"::FLOAT AS price_change_pct_60d,
            -- flattened.value:"price_change_pct_90d"::FLOAT AS price_change_pct_90d,
            -- flattened.value:"share_price_percent_all_time_high"::FLOAT AS
            -- share_price_percent_all_time_high,
            -- flattened.value:"share_price_percent_change_ytd"::FLOAT AS
            -- share_price_percent_change_ytd,
            -- flattened.value:"share_price_percent_change_52_weeks"::FLOAT AS
            -- share_price_percent_change_52_weeks,
            -- flattened.value:"ev_over_gross_profit_run_rate"::FLOAT AS
            -- ev_over_gross_profit_run_rate,
            -- flattened.value:"share_price_percent_of_low"::FLOAT AS
            -- share_price_percent_of_low,
            flattened.value:"net_new_arr"::float as net_new_arr,  -- new with Jon Ma
            -- flattened.value:"net_new_arr_growth"::FLOAT AS net_new_arr_growth,
            -- flattened.value:"operating_yield"::FLOAT AS operating_yield,
            -- flattened.value:"sales_marketing_yield"::FLOAT AS sales_marketing_yield,
            -- flattened.value:"last_year_ticker_quarter_net_new_arr"::FLOAT AS
            -- last_year_ticker_quarter_net_new_arr,
            -- flattened.value:"ltm_computed_opex"::FLOAT AS ltm_computed_opex,
            -- flattened.value:"previous_quarter_computed_opex"::FLOAT AS
            -- previous_quarter_computed_opex,
            -- flattened.value:"two_quarters_ago_computed_opex"::FLOAT AS
            -- two_quarters_ago_computed_opex,
            -- flattened.value:"three_quarters_ago_computed_opex"::FLOAT AS
            -- three_quarters_ago_computed_opex,
            -- flattened.value:"ltm_sales_marketing"::FLOAT AS ltm_sales_marketing,
            -- flattened.value:"revenue_2023"::FLOAT AS revenue_2023,
            -- flattened.value:"revenue_growth_2022"::FLOAT AS revenue_growth_2022,
            flattened.value:"revenue_estimates_2024"::float as revenue_estimates_2024,  -- new with Jon Ma
            flattened.value:"revenue_estimates_2025"::float as revenue_estimates_2025  -- new with Jon Ma
        -- flattened.value:"ev_over_2024_estimates"::FLOAT AS ev_over_2024_estimates,
        -- flattened.value:"estimates_growth_2024"::FLOAT AS estimates_growth_2024
        from latest_data, lateral flatten(input => data) as flattened
    )
select *
from flattened_data
order by ticker asc, calendar_year desc, calendar_quarter desc
