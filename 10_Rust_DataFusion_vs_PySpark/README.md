#### Datafusion vs PySpark: performance comparison and other thoughts

Can Rust's DataFusion challenge Spark's dominance in data engineering? DataFusion's recent performance improvements are intriguing.

#### Datafusion & Spark
DataFusion is a fast, extensible query engine for Rust-based data systems. It uses Apache Arrow for in-memory data processing and is part of the Apache Arrow project. It supports CSV, Parquet, JSON, Avro and offering customization with a great community.

[Datafusion docs](https://arrow.apache.org/datafusion/user-guide/introduction.html)

Apache Spark is an open-source tool used for big data processing. It's essentially a powerful engine designed to handle massive datasets across clusters of computers. It's definetely most popular Big Data tool.

[Spark docs](https://spark.apache.org/docs/latest/)


#### Performance comparison

Dataset: [ciclistic-trip-data](https://www.kaggle.com/datasets/chihchungwuo/cyclistic-trip-data)

System:

