#### Datafusion vs PySpark: performance comparison and other thoughts

Large part of data engineering world stand on Python ecosystem. And Apache Spark is really the king that brought DataFrames to the masses.I wanted to check out if Rust based Datafusion can compete with the king in performance and efficiency because Datafusion previous year made significant performace improvements.

#### Datafusion
DataFusion is a fast, extensible query engine for Rust-based data systems. It uses Apache Arrow for in-memory data processing and is part of the Apache Arrow project.

DataFusion is a fast SQL engine with Python bindings, supporting CSV, Parquet, JSON, Avro and offering customization with a great community.

[Datafusion docs](https://arrow.apache.org/datafusion/user-guide/introduction.html)


#### Performance comparison
Used dataset from Kaggle: [ciclistic-trip-data](https://www.kaggle.com/datasets/chihchungwuo/cyclistic-trip-data)
