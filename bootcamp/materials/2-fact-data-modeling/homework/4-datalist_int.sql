WITH
    monthly_dates AS (
        -- Generate a list of dates for the month (e.g., March 2023)
        SELECT generate_series(
                DATE '2023-01-01', DATE '2023-01-31', '1 day'
            ) AS valid_date
    ),
    activity_bitmapping AS (
        SELECT uc.user_id, uc.device_id, ba.browser_type, ARRAY_AGG(
                CASE
                    WHEN md.valid_date = ANY (ba.activity_dates) THEN 1::bigint << (
                        EXTRACT(
                            DAY
                            FROM md.valid_date
                        )::int - 1
                    )
                    ELSE 0
                END
            ) AS bit_positions
        FROM
            user_devices_cumulated uc
            CROSS JOIN LATERAL UNNEST(uc.device_activity_datelist) AS ba (browser_type, activity_dates)
            CROSS JOIN monthly_dates md
        GROUP BY
            uc.user_id,
            uc.device_id,
            ba.browser_type
    ),
    unnested_bits AS (
        SELECT
            user_id,
            device_id,
            browser_type,
            UNNEST(bit_positions) AS bit_value
        FROM activity_bitmapping
    ),
    aggregated_bits AS (
        SELECT
            user_id,
            device_id,
            browser_type,
            BIT_OR(bit_value)::BIT(32) AS datelist_int
        FROM unnested_bits
        GROUP BY
            user_id,
            device_id,
            browser_type
    )
SELECT *
FROM aggregated_bits;