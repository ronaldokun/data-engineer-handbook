INSERT INTO
    user_devices_cumulated (
        user_id,
        device_id,
        device_activity_datelist
    )
WITH
    device_data AS (
        SELECT d.device_id, d.browser_type
        FROM devices d
        WHERE
            d.device_id IS NOT NULL
    ),
    event_data AS (
        SELECT e.user_id, e.device_id, DATE_TRUNC(
                'day', e.event_time::TIMESTAMP
            )::DATE AS event_date
        FROM events e
        WHERE
            e.device_id IS NOT NULL
            AND e.user_id IS NOT NULL
    ),
    joined_data AS (
        SELECT ed.user_id, ed.device_id, dd.browser_type, ed.event_date
        FROM event_data ed
            INNER JOIN device_data dd ON ed.device_id = dd.device_id
    ),
    browser_aggregates AS (
        SELECT jd.user_id, jd.device_id, jd.browser_type, ARRAY_AGG(
                DISTINCT jd.event_date
                ORDER BY jd.event_date
            ) AS active_dates
        FROM joined_data jd
        GROUP BY
            jd.user_id,
            jd.device_id,
            jd.browser_type
    ),
    device_aggregates AS (
        SELECT ba.user_id, ba.device_id, ARRAY_AGG(
                ROW (
                    ba.browser_type, ba.active_dates
                )::browser_activity
            ) AS device_activity_datelist
        FROM browser_aggregates ba
        GROUP BY
            ba.user_id,
            ba.device_id
    )
SELECT da.user_id, da.device_id, da.device_activity_datelist
FROM device_aggregates da;