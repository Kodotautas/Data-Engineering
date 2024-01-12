### Python and Rust Performance for Data Engineering

**Stuff:**
Python 3.12.0
Rust 1.75.0
AMD® Ryzen 5 6600hs, 16 GB
Pop!_OS 22.04 LTS

**Dataset:** 
[Chess games, 37 million rows, size: 4.4 GB, format: .csv](https://www.kaggle.com/datasets/arevel/chess-games) 

**Results:**

| Operation | Task | Python | Rust | Rust +- % | Comments |
| --------- | ---- | -------- | ------ | ------ |-|
| **File I/O**  | Read with Pandas | 46 s | | +920% | Python Pandas vs Rust Polars
|           | Read with Polars | 21 s | 5 s| +420% | 
| **Clean & transform** | Read & remove nulls Pandas | 48 s || +50% |Python Pandas vs Rust Polars
| | Read & remove nulls Polars | 35 s | 32 s | +9%
| **Aggregation**   | Read & filter Pandas | 43 s || +614% | Python Pandas vs Rust Polars
| | Read & filter Polars | 28 s | 7 s | +400%
| **Encoding / decoding** | Convert .csv to .arrow Pandas | 62 s || +344% | Python Pandas vs Rust csv2arrow crate
|| Convert .csv to .arrow Polars | 20 s | 18 s | +11% |
| **Transfer** | Bigquery -> .txt | 4 s | 5 s | -20% |
|| .csv -> Bigquery | 214 s | 13 s | +1646% 
| **Real-time processing** | Average latency between Server & Client | 0.356 ms | 0.322 ms | +11% |
|| **Average (Py use Pandas):**| **70 s** | **13 s** | **+538%** |
|| **Average (Py use Polars):**| **54 s** | **13 s** | **+415%** |