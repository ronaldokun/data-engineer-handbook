-- What is the average number of web events of a session from a user on Tech Creator?
-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io, 'www.dataexpert.io', 'bootcamp.techcreator.io')

SELECT host, AVG(events_count) as avg_events_per_session
FROM web_sessions
WHERE
    host IN (
        'zachwilson.techcreator.io',
        'zachwilson.tech',
        'lulu.techcreator.io',
        'www.dataexpert.io',
        'bootcamp.techcreator.io'
    )
GROUP BY
    host;