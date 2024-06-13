{{ config(snowflake_warehouse="HELIUM") }}

SELECT
  o.date,
  o.onboard_fees + n.network_fees as fees,
  o.chain,
  o.protocol
FROM
{{ ref("fact_helium_onboard_fees_silver") }} o
LEFT JOIN
{{ ref("fact_helium_network_fees_silver")}} n
ON o.date = n.date