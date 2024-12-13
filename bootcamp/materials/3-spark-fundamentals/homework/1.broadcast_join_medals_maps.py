from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

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

# check which df is bigger
matches_df.count(), maps_df.count()

# Broadcast joins
matches_with_map = matches_df.join(broadcast(maps_df), matches_df["mapid"] == maps_df["mapid"], "inner")

medals_matches_players_df.count(), medals_df.count()

medals_matches_with_medals = medals_matches_players_df.join(
                                    broadcast(medals_df), medals_matches_players_df["medal_id"] == medals_df["medal_id"],
                                    "inner")

medals_with_maps_joined = medals_matches_with_medals.join(
                            broadcast(matches_with_map), medals_matches_with_medals["match_id"] == matches_with_map["match_id"], "inner")