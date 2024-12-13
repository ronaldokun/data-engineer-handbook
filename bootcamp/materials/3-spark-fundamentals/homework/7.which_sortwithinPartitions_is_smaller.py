from pathlib import Path
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("HomeWork3")
         .getOrCreate()
        )

# Disable automatic broadcast joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

folder = Path("/home/iceberg/data/")
match_details_path = f"{folder}/match_details.csv"
matches_path = f"{folder}/matches.csv"
medals_path = f"{folder}/medals.csv"
medals_matches_players_path = f"{folder}/medals_matches_players.csv"
maps_path = f"{folder}/maps.csv"

# Reading CSV files into DataFrames
match_details_df = spark.read.csv(match_details_path, header=True, inferSchema=True)
matches_df = spark.read.csv(matches_path, header=True, inferSchema=True)
medals_matches_players_df = spark.read.csv(medals_matches_players_path, header=True, inferSchema=True)
medals_df = spark.read.csv(medals_path, header=True, inferSchema=True)
maps_df = spark.read.csv(maps_path, header=True, inferSchema= True)

# Save bucketed tables
match_details_df.write.format("parquet") \
    .bucketBy(16, "match_id") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.bucketed_match_details")

matches_df.write.format("parquet") \
    .bucketBy(16, "match_id") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.bucketed_matches")

medals_matches_players_df.write.format("parquet") \
    .bucketBy(16, "match_id") \
    .mode("overwrite") \
    .saveAsTable("bootcamp.bucketed_medals_matches_players")

# Load bucketed tables (if needed for future queries)
bucketed_match_details = spark.table("bootcamp.bucketed_match_details")
bucketed_matches = spark.table("bootcamp.bucketed_matches")
bucketed_medals_matches_players = spark.table("bootcamp.bucketed_medals_matches_players")

# Perform the join on bucketed tables
joined_df = bucketed_match_details.join(
    bucketed_matches, "match_id", "inner"
).join(
    bucketed_medals_matches_players, "match_id", "inner"
)

# Sort by playlist_id within partitions and save
sorted_by_playlist = joined_df.sortWithinPartitions("playlist_id")
# Write the sorted DataFrames to storage and compare file sizes
sorted_by_playlist.write.mode("overwrite").saveAsTable("bootcamp.sorted_by_playlist")

# Sort by mapid within partitions and save
sorted_by_map = joined_df.sortWithinPartitions("mapid")
sorted_by_map.write.mode("overwrite").saveAsTable("bootcamp.sorted_by_map")

# To compare the file sizes, run the following SQL query
compare_file_sizes = """

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'by_playlist' 
FROM bootcamp.sorted_by_playlist.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'by_map' 
FROM bootcamp.sorted_by_map.files
"""

# Sorting by playlist is slightly smaller than sorting by map