-- Cohort retention by segment: for each segment, how many customers are
-- currently in it vs. how many ever passed through it.
SELECT
    segment,
    COUNT(*) FILTER (WHERE is_current) AS customers_currently_in_segment,
    COUNT(DISTINCT customer_id) AS customers_ever_in_segment,
    ROUND(
        COUNT(*) FILTER (WHERE is_current) * 100.0 / NULLIF(COUNT(DISTINCT customer_id), 0), 1
    ) AS retention_pct
FROM dim_customer
GROUP BY segment
ORDER BY customers_currently_in_segment DESC;
