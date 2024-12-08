WITH
    aggregated_host_data AS (
        -- Step 1: Aggregate event data for each host
        SELECT
            e.host as host_name,
            ARRAY_AGG(
                DISTINCT DATE_TRUNC(
                    'day',
                    e.event_time::TIMESTAMP
                )::DATE
                ORDER BY DATE_TRUNC(
                        'day', e.event_time::TIMESTAMP
                    )::DATE
            ) AS host_activity_datelist
        FROM events e
        WHERE
            e.host IS NOT NULL
        GROUP BY
            e.host
    )
INSERT INTO
    hosts_cumulated (
        host_name,
        host_activity_datelist
    )
SELECT ahd.host_name, ahd.host_activity_datelist
FROM aggregated_host_data ahd
ON CONFLICT (host_name) DO
UPDATE
SET
    host_activity_datelist = (
        -- Merge new activity dates with existing ones
        SELECT ARRAY(
                SELECT DISTINCT
                    d
                FROM UNNEST(
                        hosts_cumulated.host_activity_datelist || EXCLUDED.host_activity_datelist
                    ) AS d
                ORDER BY d
            )
    );

SELECT * FROM hosts_cumulated;