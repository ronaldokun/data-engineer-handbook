with
    daily_hits as (
        -- Aggregate daily data from `events` table for the given day
        SELECT
            DATE_TRUNC(
                'month',
                event_time::timestamp
            ) as month,
            host,
            ARRAY[count(1)] as hit_array,
            ARRAY[count(distinct user_id)] as unique_visitors
        FROM events
        WHERE
            DATE (event_time) = DATE ('2023-01-01')
            AND host IS NOT NULL
        GROUP BY
            DATE_TRUNC(
                'month',
                event_time::timestamp
            ),
            host
    ),
    merged_data as (
        -- Merge the daily data with yesterday's data
        SELECT
            COALESCE(y.month, d.month) AS month,
            COALESCE(y.host, d.host) AS host,
            CASE
                WHEN y.hit_array IS NOT NULL THEN y.hit_array || ARRAY[
                    COALESCE(d.hit_array, ARRAY[0])
                ]
                ELSE COALESCE(d.hit_array, ARRAY[0])
            END AS hit_array,
            CASE
                WHEN y.unique_visitors IS NOT NULL THEN y.unique_visitors || ARRAY[
                    COALESCE(d.unique_visitors, ARRAY[0])
                ]
                ELSE COALESCE(d.unique_visitors, ARRAY[0])
            END AS unique_visitors
        FROM
            daily_hits d
            FULL OUTER JOIN host_activity_reduced y ON d.host = y.host
    )
INSERT INTO
    host_activity_reduced (
        month,
        host,
        hit_array,
        unique_visitors
    )
SELECT month, host, hit_array, unique_visitors
FROM merged_data
ON CONFLICT (month, host) DO
UPDATE
SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;