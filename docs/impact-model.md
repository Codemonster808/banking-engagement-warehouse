# Impact model — assumptions

| # | Assumption | Value used | Source (fill in before publishing) |
|---|---|---|---|
| 1 | Analyst hours/month spent debugging unreproducible cohort reports | TODO | TODO — cite a public data-eng survey or blog post |
| 2 | Fully loaded analyst hourly cost | TODO | TODO |
| 3 | Number of cohort reports produced per month | TODO | TODO |

## Calculation

```
hours_saved_per_month = analyst_hours_lost_to_debugging * fix_effectiveness_pct
value_per_month        = hours_saved_per_month * hourly_cost
value_per_year          = value_per_month * 12
```

## Rule for this file

Never change the README's "Modeled business impact" number without updating this file in the same commit.
