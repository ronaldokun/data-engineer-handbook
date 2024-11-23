CREATE TABLE actors_history_scd (
    actorid TEXT NOT NULL,              -- Unique identifier for each actor
    actor TEXT NOT NULL,                -- Name of the actor
    quality_class quality_rating,       -- Performance quality class
    is_active BOOLEAN,                  -- Whether the actor is active
    start_date DATE NOT NULL,           -- When this record became effective
    end_date DATE,                      -- When this record was replaced, NULL if current
    is_current BOOLEAN DEFAULT TRUE,    -- Indicates whether this is the current record
    PRIMARY KEY (actorid, start_date)   -- Composite key for tracking history
);