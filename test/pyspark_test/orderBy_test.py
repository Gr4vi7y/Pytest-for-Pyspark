def test_orderBy_dataframe(spark_session):
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    
    data = [("James","","Smith","36636","M",3000),
    	     ("Michael","Rose","","40288","M",4000),
             ("Robert","","Williams","42114","M",4000),
             ("Maria","Anne","Jones","39192","F",4000),
             ("Jen","Mary","Brown","","F",-1)]

    schema = StructType([ \
             StructField("firstname",StringType(),True), \
    	     StructField("middlename",StringType(),True), \
    	     StructField("lastname",StringType(),True), \
    	     StructField("id", StringType(), True), \
    	     StructField("gender", StringType(), True), \
    	     StructField("salary", IntegerType(), True) \
                       ])
 
    df = spark_session.createDataFrame(data=data,schema=schema)
    assert df.select("firstname").orderBy("firstname").first()[0] == "James"