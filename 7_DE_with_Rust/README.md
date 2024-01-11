### Data engineering main tasks performace comparison between Python and Rust

**Equipment:**
Python 3.12.0
Rust 1.75.0
Laptop: AMD® Ryzen 5 6600hs, 16 GB
OS: Pop!_OS 22.04 LTS

**Dataset:** 
[Chess games, 31 million rows, size: 4.4 GB, format: .csv](https://www.kaggle.com/datasets/arevel/chess-games) 

**Results:**

| Operation | Task | Python | Rust | Rust faster % |
| --------- | ---- | -------- | ------ | ------ |
| File I/O  | Read with Pandas | 46 s | | +418% | 
|           | Read with Polars | 21 s | 11 s| +91% | 
| Data clean & transform | Read & remove nulls Pandas | 48 s || +50%
| Data clean & transform | Read & remove nulls Polars | 35 s | 32 s | +9%
| Data aggregation   | Read & filter Pandas | 43 s || +614%
| Data aggregation   | Read & filter Polars | 28 s | 7 s | +400%
| Data encoding / decoding | Convert .csv to .arrow Pandas | 62 s || +344% |
| Data encoding / decoding | Convert .csv to .arrow Polars | 20 s | 18 s | +11% |
| Data transfer | From GCP Bigquery to .txt | 4 s | 5 s | -20% 
| Data transfer | From .csv to Bigquery | 214 s | 13 s | +1646% 
| Real-time data processing | Average latency between server & client | 0.356 ms | 0.322 ms | +11% |