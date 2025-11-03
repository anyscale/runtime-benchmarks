## 🔁 OSS vs Anyscale Runtime: TPCH Q1 Benchmark

### 📦 Dataset Details

These tests were run on TPCH lineitem dataset on Ray 2.51.0 version.

### GKE Cluster Specs

- **1 head node** — 8 CPUs, 32 GB RAM  
- **4 worker nodes** — each with 32 CPUs, 128 GB RAM  

#### 📊 Benchmark Results: Anyscale Runtime vs OSS 

To reproduce the numbers you can run `python tpch_q1.py --sf 1000`  (1000 for 1TB, 100 for 100GB).

| Size   | Anyscale Runtime (s) | Ray OSS (s) | 
|--------|--------|--------------------|
| 100 GB  | 21.8  | 107.64              |
| 1 TB  | 99.1  | 913.15             |
