def test_count_dataframe(spark_session):
    df = spark_session.createDataFrame([("Java", "20000"), ("Python", "100000"), ("Scala", "3000")])
    assert df.count() == 3


