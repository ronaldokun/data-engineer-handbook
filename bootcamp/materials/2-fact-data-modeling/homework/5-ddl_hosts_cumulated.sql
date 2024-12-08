DROP TABLE hosts_cumulated;

CREATE TABLE hosts_cumulated (
    host_name TEXT,
    host_activity_datelist DATE[], -- Array of browser_activity
    PRIMARY KEY (host_name)
);