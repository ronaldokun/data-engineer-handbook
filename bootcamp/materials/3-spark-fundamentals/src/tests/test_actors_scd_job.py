from chispa.dataframe_comparer import assert_df_equality

from ..jobs.actors_scd_job_unique import do_actor_scd_transformation
from collections import namedtuple

ActorYear = namedtuple("ActorFilm", "actorid actor films quality_class is_active current_year")
ActorScd = namedtuple("ActorScd", "actorid actor quality_class is_active start_date end_date is_current")
def test_scd_generation(spark):
    source_data = [
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2005),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2006),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2007),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2008),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2009),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2010),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2011),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2012),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'average', True, 2013),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2014),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'average', True, 2015),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'average', True, 2016),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'average', True, 2017),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2018),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2019),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2020),
        ActorYear('nm1265067', '50 Cent', [{'film': "Get Rich or Die Tryin'", 'votes': 44370, 'rating': 5, 'filmid': 'tt0430308'}], 'bad', True, 2021)
    ]

    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)

    expected_data = [
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2005-01-01, 2005-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2006-01-01, 2006-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', False, 2007-01-01, 2007-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2008-01-01, 2008-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2009-01-01, 2009-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2010-01-01, 2010-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2011-01-01, 2011-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2012-01-01, 2012-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'average', True, 2013-01-01, 2013-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2014-01-01, 2014-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'average', True, 2015-01-01, 2015-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'average', True, 2016-01-01, 2016-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'average', False, 2017-01-01, 2017-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2018-01-01, 2018-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', True, 2019-01-01, 2019-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', False, 2020-01-01, 2020-12-31, False),
        ActorScd('nm1265067', '50 Cent', 'bad', False, 2021-01-01, None, True),
    ]

    expected_df = spark.createDataFrame(expected_data)

    assert_df_equality(actual_df, expected_df)

        
