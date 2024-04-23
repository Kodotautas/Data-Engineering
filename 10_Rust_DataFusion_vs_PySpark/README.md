#### Datafusion vs PySpark: performance comparison and other thoughts

Can Rust's DataFusion challenge Spark's dominance in data engineering? DataFusion's recent performance improvements are intriguing.

#### Datafusion & Spark
DataFusion is a fast query engine for Rust that uses Apache Arrow for speedy data analysis. It supports common data formats and offers customization for specific needs, with a strong community for support.

[Datafusion docs](https://arrow.apache.org/datafusion/user-guide/introduction.html)

Apache Spark: Open-source big data engine for handling massive datasets across clusters, making it a popular choice.

[Spark docs](https://spark.apache.org/docs/latest/)

Feature | Apache DataFusion (Rust) | Apache Spark |
--- | --- | --- |
Processing Model | Single-node, in-memory | Distributed, in-memory and out-of-memory |
Language | Rust | Scala (primary), Java, Python, R |
Scalability | Limited (can be distributed with Ballista) | Highly scalable |
Supported Data Sources | CSV, Parquet, AVRO, JSON | Wide variety including CSV, Parquet, JSON, JDBC, and more |
Performance | Potentially faster for smaller datasets due to Rust's memory management | Generally faster for large datasets due to distributed processing |
Ease of Use | Simpler API, easier to learn | More complex API with a larger learning curve |
Ecosystem | Smaller, growing ecosystem | Large, mature ecosystem with a wide range of libraries and tools
Maturity | Relatively new project (5 years) | Established and widely used |
Use Cases | analytics, prototyping, embedded analytics | Large-scale data processing, machine learning, real-time analytics3 |

#### Which is faster?
Dataset (12 csv files, 19 - 153 MB each, total 1.1 Gb): [ciclistic-trip-data](https://www.kaggle.com/datasets/chihchungwuo/cyclistic-trip-data)


increased: 4232 files, 192 Gb

System:
AMD® Ryzen 5 6600hs creator edition × 12
16.0 GiB
Pop!_OS 22.04 LTS
