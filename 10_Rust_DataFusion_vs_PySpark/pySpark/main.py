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
            r.rideable_type,
            r2.member_casual,
            count(r3.ride_id) as total_rides,
            avg(r4.start_lat) as avg_start_lat,
            avg(r5.start_lng) as avg_start_lng
        from rides r
        left join rides r2 on r.ride_id = r2.ride_id
        left join rides r3 on r.ride_id = r3.ride_id
        left join rides r4 on r.ride_id = r4.ride_id
        left join rides r5 on r.ride_id = r5.ride_id
        group by r.rideable_type, r2.member_casual
        """
        )
    
    t.show()
    stop = datetime.now()
    print("Elapsed Time: ", stop-start, "s")


if __name__ == '__main__':
    # print("PySpark - No SQL")
    # no_sql()
    print("PySpark - SQL")
    sql()