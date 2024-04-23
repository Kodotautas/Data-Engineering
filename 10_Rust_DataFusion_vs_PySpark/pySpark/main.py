from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

def no_sql():
    # Test PySpark with no SQL
    spark = SparkSession.builder.appName('pySpark').master('local[6]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    start = datetime.now()
    df = spark.read.csv("../rust-datafusion/data/*csv", header='true')

    trans = df.groupBy('rideable_type').agg(F.count('start_lat'))
    trans.show()

    stop = datetime.now()
    print("Elapsed Time: ", stop-start, "s")

def sql():
    # Test PySpark with SQL and joins
    spark = SparkSession.builder.appName('pySpark').master('local[6]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    start = datetime.now()
    df = spark.read.csv("../rust-datafusion/data/*csv", header='true')
    df.createOrReplaceTempView("rides")

    t = spark.sql(
        """
        select 
            rideable_type,
            member_casual,
            count(ride_id) as total_rides,
            avg(start_lat) as avg_start_lat,
            avg(start_lng) as avg_start_lng
        from rides r
        where start_station_name = 'Michigan Ave & Jackson Blvd'
        group by rideable_type, member_casual
        """
        )
    
    t.show()
    stop = datetime.now()
    print("Elapsed Time: ", stop-start, "s")


if __name__ == '__main__':
    print("PySpark - No SQL")
    no_sql()
    print("PySpark - SQL")
    sql()