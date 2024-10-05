### BigQuery History-Based Optimizations Feature

BigQuery's history-based optimizations leverage past query executions to enhance performance, reducing slot time and latency. For instance, an initial query might take 45 seconds, but subsequent executions could drop to 20 seconds as optimizations are applied. This process continues until no further optimizations are possible.

**!** History-based optimizations are applied only when beneficial and revoked if ineffective.


### Enabling History-Based Optimizations

```sql
ALTER PROJECT PROJECT_NAME
SET OPTIONS (
  `region-LOCATION.default_query_optimizer_options` = 'adaptive=on'
);
```

Replace `LOCATION` and `PROJECT_NAME` with your own.


### Checking History-Based Optimized Queries

The query below returns all optimized queries and the percentage of execution time saved in the last 30 days.

```
  WITH
    jobs AS (
      SELECT
        *,
        query_info.query_hashes.normalized_literals AS query_hash,
        TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS elapsed_ms,
        IFNULL(
          ARRAY_LENGTH(JSON_QUERY_ARRAY(query_info.optimization_details.optimizations)) > 0,
          FALSE)
          AS has_history_based_optimization,
      FROM region-LOCATION.INFORMATION_SCHEMA.JOBS_BY_PROJECT
      WHERE EXTRACT(DATE FROM creation_time) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ),
    most_recent_jobs_without_history_based_optimizations AS (
      SELECT *
      FROM jobs
      WHERE NOT has_history_based_optimization
      QUALIFY ROW_NUMBER() OVER (PARTITION BY query_hash ORDER BY end_time DESC) = 1
    )
  SELECT
    job.job_id,
    SAFE_DIVIDE(
      original_job.elapsed_ms - job.elapsed_ms,
      original_job.elapsed_ms) AS percent_execution_time_saved,
    job.elapsed_ms AS new_elapsed_ms,
    original_job.elapsed_ms AS original_elapsed_ms,
  FROM jobs AS job
  INNER JOIN most_recent_jobs_without_history_based_optimizations AS original_job
    USING (query_hash)
  WHERE
    job.has_history_based_optimization
    AND original_job.end_time < job.start_time
  ORDER BY percent_execution_time_saved DESC
```

**Official Documentation:** [link](https://cloud.google.com/bigquery/docs/history-based-optimizations)

### Difficulty in Testing
It's challenging to test this feature and its performance on personal datasets due to limited data volumes and query variety. However, enabling it is free, so you can try it on your datasets. Wish it will help you to get a better performance for your queries! 💪