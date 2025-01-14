from chispa.dataframe_comparer import assert_df_equality

from ..jobs.actors_streak_job import do_actors_streak_identification
from collections import namedtuple

ActorYear = namedtuple("ActorYear", "actor quality_class current_year")
ActorStreak = namedtuple("ActorStreak", "actor quality_class streak_identifier start_date end_date")
def test_streak_generation(spark):
    source_data = [
        ActorYear('50 Cent', 'bad', 2005),
        ActorYear('50 Cent', 'bad', 2006),
        ActorYear('50 Cent', 'bad', 2007),
        ActorYear('50 Cent', 'bad', 2008),
        ActorYear('50 Cent', 'bad', 2009),
        ActorYear('50 Cent', 'bad', 2010),
        ActorYear('50 Cent', 'bad', 2011),
        ActorYear('50 Cent', 'bad', 2012),
        ActorYear('50 Cent', 'average', 2013),
        ActorYear('50 Cent', 'bad', 2014),
        ActorYear('50 Cent', 'average', 2015),
        ActorYear('50 Cent', 'average', 2016),
        ActorYear('50 Cent', 'average', 2017),
        ActorYear('50 Cent', 'bad', 2018),
        ActorYear('50 Cent', 'bad', 2019),
        ActorYear('50 Cent', 'bad', 2020),
        ActorYear('50 Cent', 'bad', 2021)
    ]

    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_streak_identification(spark, source_df)

    expected_data = [
        ActorStreak('50 Cent', 'average', 2, 2013, 2013),
        ActorStreak('50 Cent', 'average', 4, 2015, 2017),
        ActorStreak('50 Cent', 'bad', 1, 2005, 2012),
        ActorStreak('50 Cent', 'bad', 3, 2014, 2014),
        ActorStreak('50 Cent', 'bad', 5, 2018, 2021),
        
    ]

    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df, expected_df)

        
