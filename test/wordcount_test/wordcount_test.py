import pytest
import wordcount

pytestmark = pytest.mark.usefixtures("spark_session")

def test_do_word_counts(spark_session):
    test_input = [
        ' hello spark ',
        ' hello again spark spark'
    ]

    input_rdd = spark_session.sparkContext.parallelize(test_input, 1)
    results = wordcount.do_word_counts(input_rdd)

    expected_results = {'hello':2, 'spark':3, 'again':1}  
    assert results == expected_results