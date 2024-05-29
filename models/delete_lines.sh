#!/bin/bash
awk '
/- dbt_expectations.expect_column_values_to_be_within_n_moving_stdevs:/ {
    for (i=0; i<8; i++) {getline}
    next
}
{print}
' "$1" > temp && mv temp "$1"