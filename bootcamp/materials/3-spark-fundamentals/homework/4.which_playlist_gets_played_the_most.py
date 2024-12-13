from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import  countDistinct, desc

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


# Aggregate to calculate the count of matches per playlist
playlist_count_df = joined_df.groupBy(["playlist_id"]) \
    .agg(countDistinct("match_id").alias("num_distinct_matches")) \
    .orderBy(desc("num_distinct_matches"))

# Find the playlist with the maximum number of matches
top_playlist = playlist_count_df.first()

print(f"Playlist with the most plays: {top_playlist['playlist_id']} ({top_playlist['num_distinct_matches']} matches)")

# Playlist with the most plays: f72e0ef0-7c4a-4307-af78-8e38dac3fdba (7640 matches)
