## 🔁 OSS vs Turbo: TPCH Q1 Benchmark

### Cluster Specs

- **1 head node** — 8 CPUs, 32 GB RAM  
- **16 worker nodes** — each with 32 CPUs, 128 GB RAM  

### 📦 Dataset Details

These tests were run on TPCH lineitem dataset.

### 📊 Benchmark Results: Turbo vs OSS 

To reproduce the numbers you can run `python tpch_q1.py --sf 1000`  (1000 for 1TB, 100 for 100GB).

| Size   | Ray Turbo (s) | Ray OSS (s) | 
|--------|--------|--------------------|
| 100 GB  | 22.96  | 58.63              |
| 1 TB  | 113.78  | 448.51             |

