### Rust Datafusion vs PySpark: 10 billions rows performance comparison
Can Rust's DataFusion challenge Spark's dominance in data engineering? DataFusion's recent performance improvements are intriguing.

#### Datafusion & Spark
DataFusion ([docs](https://arrow.apache.org/datafusion/user-guide/introduction.html)) is a fast query engine for Rust that uses Apache Arrow for speedy data analysis. It supports common data formats and offers customization for specific needs, with a strong community for support.



Apache Spark ([docs](https://spark.apache.org/docs/latest/)): Open-source big data engine for handling massive datasets across clusters, making it a popular choice.

##### Comparison table
Feature | Apache DataFusion (Rust) | Apache Spark |
--- | --- | --- |
Processing Model | Single-node, in-memory | Distributed, in-memory and out-of-memory |
Language | Rust | Scala (primary), Java, Python, R |
Scalability | Limited but on roadmap (can be distributed with Ballista) | Highly scalable |
Supported Data Sources | CSV, Parquet, AVRO, JSON | Wide variety including CSV, Parquet, JSON, JDBC, and more |
Performance | Faster? | Slower? |
Ease of Use | Simpler API, easier to learn | More complex API with a larger learning curve |
Ecosystem | Smaller, growing ecosystem | Large, mature ecosystem with a wide range of libraries and tools
Maturity | Relatively new project (~5 years)| Established and widely used |
Use Cases | Large scale analytics, prototyping, embedded analytics | Large-scale data processing, machine learning, real-time analytics |


### So... Which is faster using 1.1 GB dataset?
##### System:
AMD® Ryzen 5 6600hs creator edition × 12
16.0 GiB
Pop!_OS 22.04 LTS


##### Dataset: [ciclistic-trip-data](https://www.kaggle.com/datasets/chihchungwuo/cyclistic-trip-data) (12 csv files, 19 - 153 MB each, total 1.1 GB)


#### Test 1: Group by & count with functions
Find out how many cyclists in each class was using tools functions.

###### PySpark
took almost 6 seconds to get count's by bike type.

<div align="center">
  <img src="./pySpark/img/spark-nosql-normal-dataset-table.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./pySpark/img/spark-nosql-normal-dataset-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div>


###### Datafusion
took almost 313 ms which is faster 19x faster than PySpark!

<div align="center">
  <img src="./rust-datafusion/img/datafusion-nosql-standart-dataset.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./rust-datafusion/img/datafusion-nosql-normal-dataset-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<!-- #### Test 2: Group by, filter, calculations using SQL

Use SQL and little more calculations:
```
select 
  rideable_type,
  member_casual,
  count(ride_id) as total_rides,
  avg(start_lat) as avg_start_lat,
  avg(start_lng) as avg_start_lng
from rides
where start_station_name = 'Michigan Ave & Jackson Blvd'
group by rideable_type, member_casual
```

###### PySpark
Took also almost 6 seconds, the same as previous.

<div align="center">
  <img src="./pySpark/img/spark-sql-normal-dataset-table.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./pySpark/img/spark-sql-normal-dataset-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div>

###### Datafusion
Took 427 ms which is 12x faster than PySpark.

<div align="center">
  <img src="./rust-datafusion/img/datafusion-sql-normal-dataset.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./rust-datafusion/img/datafusion-sql-normal-dataset-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div> -->

### ok... Multiply our dataset up to 10 billion rows!
I multiplied main dataset by 350 times and it increased to 4200 files and total 192 GB size.

<!-- #### Test 2: Group by & count with functions (10 bill rows) -->
<!-- ###### PySpark
PySpark took 4 min. 5 seconds to calculate.

<div align="center">
  <img src="./pySpark/img/spark-nosql-10-bill-dataset.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./pySpark/img/spark-nosql-10-bill-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div>

###### Datafusion

Rust based Datafusion took 68 seconds which is 3.8x faster.

<div align="center">
  <img src="./rust-datafusion/img/datafusion-nosql-10-bill-dataset-count.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./rust-datafusion/img/datafusion-nosql-10-bill-dataset-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div> -->


#### Test 2: Group by, filter, calculations using SQL (10 bill rows)

SQL for second test:
```
select 
  rideable_type,
  member_casual,
  count(ride_id) as total_rides,
  avg(start_lat) as avg_start_lat,
  avg(start_lng) as avg_start_lng
from rides
where start_station_name = 'Michigan Ave & Jackson Blvd'
group by rideable_type, member_casual
```

###### PySpark
PySpark completed challenge in 4 min. 8 seconds.
<div align="center">
  <img src="./pySpark/img/spark-sql-10-bill-dataset.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./pySpark/img/spark-sql-10-bill-dataset-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div>

###### Datafusion
Completed it in 1 min. 31 second which is again faster than PySpark by 2.7x.

<div align="center">
  <img src="./rust-datafusion/img/datafusion-sql-10-bill-dataset.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<div align="center">
  <img src="./rust-datafusion/img/datafusion-sql-10-bill-dataset-chart.jpg" alt="BigQuery tranlated column example" width="500">
</div>

<!-- #### Conclusion
There was similar tests on internet about one year ago there PySpark beaten Datafusion. Spark is well developed, have many integrations and in many companies it's like legacy tool. Don't forget that it's integrated in cloud services like AWS ERM or GCP Dataproc. 
But after Datafusion updates situation is opposite. Projects which need performance, lower costs, stability and safety have good option - Apache Datafusion. Rust ecosystem is growing based on code share in GitHub (current 1.75% of whole code written in Rust) and currently in data engineering you can everything with it. -->

#### So... Old guard vs. rising star?

Remember the DataFusion vs. PySpark showdowns from last year? Spark, the industry OG, reigned supreme with its vast integrations and legacy status. Cloud giants like AWS and GCP even embraced it with open arms (think EMR and Dataproc).

But fast forward to today, and the tables have turned. DataFusion's been on a tear, making it a strong contender for projects prioritizing performance, cost efficiency, and rock-solid stability.

DataFusion's advantages:

- Rust FTW: The Rust ecosystem is growing (1.75% of all GitHub code!) and it offers a complete data engineering toolkit.
- Mean Machine: DataFusion delivers blazing-fast query execution, making it a dream for performance-hungry geeks.
- Lean & Mean: Compared to Spark's bulkiness, DataFusion is a lightweight champ, keeping your costs in check.

So, if you're a data ninja seeking a modern, high-performance query engine, DataFusion might be your new best friend.