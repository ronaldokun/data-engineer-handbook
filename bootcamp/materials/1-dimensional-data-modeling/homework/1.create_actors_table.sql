-- Define the composite type for films
CREATE TYPE films_struct AS (
    film TEXT,
    votes INTEGER, 
    rating INTEGER,
    filmid TEXT
);

-- Define the ENUM type for quality_class
CREATE TYPE quality_rating AS ENUM ('star', 'good', 'average', 'bad');

-- Create the actors table
CREATE TABLE actors (
    actorid TEXT,                        -- Unique identifier for each actor
    actor TEXT,                          -- Name of the actor
    films films_struct[],                -- Array of films using the composite type
    quality_class quality_rating,        -- Performance quality class
    is_active BOOLEAN DEFAULT FALSE,     -- Whether actor is active this year
    current_year INT,                    -- Year of the data
    PRIMARY KEY (actorid, current_year)
);


