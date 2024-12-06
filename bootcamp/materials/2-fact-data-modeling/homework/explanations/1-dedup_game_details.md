To implement partitioning correctly with filtering in a `SELECT` query using `ROW_NUMBER()`, you need to use a **Common Table Expression (CTE)** or a **subquery** because `ROW_NUMBER()` is evaluated after the `WHERE` clause. You can't directly use `ROW_NUMBER()` in `WHERE`. Here's how to correct your query:

### Correct Approach Using a CTE:

```sql
WITH ranked_game_details AS (
    SELECT 
        gd.*, 
        ROW_NUMBER() OVER (
            PARTITION BY gd.game_id, gd.team_id, gd.player_id
            ORDER BY gd.game_id -- or another column you prefer to order by
        ) AS row_num
    FROM game_details gd
)
SELECT * 
FROM ranked_game_details
WHERE row_num = 1;
```

### Explanation:
1. **CTE (`WITH` clause)**: The query defines a CTE `ranked_game_details` where `ROW_NUMBER()` is calculated.
2. **Partitioning and ordering**: `ROW_NUMBER()` assigns a unique number to each row in each partition of `(game_id, team_id, player_id)`, starting from `1`.
3. **Filtering**: The main query filters rows where `row_num = 1`, effectively returning the first row per partition.

### Alternative: Subquery Approach
If you prefer subqueries over CTEs, use this:

```sql
SELECT *
FROM (
    SELECT 
        gd.*, 
        ROW_NUMBER() OVER (
            PARTITION BY gd.game_id, gd.team_id, gd.player_id
            ORDER BY gd.game_id -- or another column to order by
        ) AS row_num
    FROM game_details gd
) AS subquery
WHERE row_num = 1;
```

Both approaches will correctly filter only the first row from each partition.