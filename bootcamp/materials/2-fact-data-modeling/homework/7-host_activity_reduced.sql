CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INT[],
    unique_visitors INT[],
    PRIMARY KEY (month, host)
);

with daily_hits as (
    SELECT host, date_trunc(
        'month', event_time::timestamp
    ) as month, count(1) as hits, count(distinct user_id) as unique_visitors
    from events
    WHERE
        DATE (event_time) = DATE ('2023-01-01')
        AND host IS NOT NULL
    GROUP BY
        host,
        month


),
yesterday_hits as (
    SELECT *
    FROM host_activity_reduced
    WHERE
        month = DATE ('2023-01-01')
)


SELECT
    COALESCE(y.month, d.month) AS month,
    COALESCE(y.host, d.host) AS host,
    CASE
        WHEN y.hits IS NOT NULL THEN y.hits || ARRAY[COALESCE(d.hits, 0)]
        WHEN y.hits IS NULL THEN ARRAY_FILL(
            0,
            ARRAY[
                COALESCE(
                    date - DATE (DATE_TRUNC('month', date)),
                    0
                )
            ]
        ) || ARRAY[COALESCE(d.hits, 0)]
    END AS hit_array,
    CASE
        WHEN y.unique_visitors IS NOT NULL THEN y.unique_visitors || ARRAY[
            COALESCE(d.unique_visitors, 0)
        ]
        WHEN y.unique_visitors IS NULL THEN ARRAY_FILL(
            0,
            ARRAY[
                COALESCE(
                    date - DATE (DATE_TRUNC('month', date)),
                    0
                )
            ]
        ) || ARRAY[
            COALESCE(d.unique_visitors, 0)
        ]
    END AS unique_visitors
FROM
    daily_hits d
    FULL OUTER JOIN yesterday_hits y ON d.host = y.host
ON CONFLICT (month, host) DO
UPDATE
SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;