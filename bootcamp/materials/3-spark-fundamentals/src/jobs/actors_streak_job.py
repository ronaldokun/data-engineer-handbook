from pyspark.sql import SparkSession

query = """
WITH streak_started AS (
	SELECT
		actor,
		current_year, 
		quality_class,
		LAG(quality_class, 1) OVER 
			(PARTITION BY actor ORDER BY current_year) <> quality_class
		OR
		LAG(quality_class, 1) OVER 
			(PARTITION BY actor ORDER BY current_year) IS NULL 
			AS did_change
	FROM actors
	),
	streak_identified AS (
		SELECT 
			actor,
			quality_class,
			current_year,
			sum(CASE WHEN did_change THEN 1 ELSE 0 END)
				OVER 
				(PARTITION BY actor ORDER BY current_year) AS streak_identifier
		FROM streak_started
	),
	aggregated AS (
		SELECT
			actor,
			quality_class,
			streak_identifier,
			MIN(current_year) AS start_date,
			MAX(current_year) AS end_date
		FROM streak_identified
		GROUP BY 1,2,3
	)
		
	SELECT * FROM aggregated
	WHERE actor = '50 Cent';
"""

def do_actors_streak_identification(spark, dataframe):
    dataframe.createOrReplaceTempView("actors_scd")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_streak") \
      .getOrCreate()
    output_df = do_actors_streak_identification(spark, spark.table("actors_scd"))
    output_df.write.mode("overwrite").insertInto("actors_streak")

