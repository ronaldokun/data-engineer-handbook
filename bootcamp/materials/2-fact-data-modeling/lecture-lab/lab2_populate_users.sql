SELECT * FROM events;

drop table users_cumulated;

CREATE TABLE users_cumulated (
    user_id TEXT,
    dates_active DATE[],
    current_dt DATE,
    PRIMARY KEY (user_id, current_dt)
);

DO $$
DECLARE
    dt date;
BEGIN
    FOR dt IN 
        SELECT generate_series(
            '2022-12-31'::date,
            '2023-01-31'::date,
            '1 day'::interval
        )
    LOOP
        INSERT INTO users_cumulated (user_id, dates_active, current_dt)
        WITH yesterday AS (
            SELECT *
            FROM users_cumulated
            WHERE current_dt = dt
        ),
        today AS (
            SELECT user_id::text, date(event_time::TIMESTAMP) as date_active
            FROM events
            WHERE date(event_time::TIMESTAMP) = dt + interval '1 day'
            AND user_id is not null
            GROUP BY user_id, date(event_time::TIMESTAMP)
        )
        SELECT 
            COALESCE(t.user_id, y.user_id) as user_id,
            CASE
                WHEN y.dates_active is null THEN array[t.date_active]::date[]
                WHEN t.date_active is null THEN y.dates_active
                ELSE array[t.date_active]::date[] || y.dates_active
            END as dates_active,
            COALESCE(t.date_active, y.current_dt + interval '1 day') as current_dt
        FROM today t
        FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;
    END LOOP;
END $$;