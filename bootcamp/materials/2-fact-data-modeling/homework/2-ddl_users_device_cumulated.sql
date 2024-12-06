CREATE TYPE browser_activity AS (
    browser_type TEXT,
    activity_dates DATE[]
);

DROP TABLE user_devices_cumulated;

CREATE TABLE user_devices_cumulated (
    user_id NUMERIC,
    device_id NUMERIC NOT NULL,
    device_activity_datelist browser_activity[], -- Array of browser_activity
    PRIMARY KEY (user_id, device_id)
);