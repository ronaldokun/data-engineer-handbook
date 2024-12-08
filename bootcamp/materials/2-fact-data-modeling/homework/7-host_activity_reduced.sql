CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INT[],
    unique_visitors INT[],
    PRIMARY KEY (month, host)
);