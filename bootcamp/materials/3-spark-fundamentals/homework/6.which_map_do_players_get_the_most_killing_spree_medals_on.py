from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import  desc, col

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


# Medal ID corresponding to "Killing Spree"
killing_spree_medal_id = 2430242797

# Filter for Killing Spree medals
killing_spree_df = joined_df.filter(col("medal_id") == killing_spree_medal_id)

# Remove duplicate (mapid, match_id) pairs to avoid double counting matches
distinct_matches_df = killing_spree_df.select("mapid", "match_id", "count").distinct()

# Aggregate to find the sum of "count" per map
killing_spree_by_map = distinct_matches_df.groupBy("mapid") \
    .agg(sum("count").alias("total_killing_spree_medals"))

# Join with maps_df to get map names
killing_spree_with_names = killing_spree_by_map.join(
    maps_df, killing_spree_by_map.mapid == maps_df.mapid, "inner"
).select(maps_df.name.alias("map_name"), "total_killing_spree_medals") \
 .orderBy(desc("total_killing_spree_medals"))

# Show the map with the most Killing Spree medals
killing_spree_with_names.show(1)

