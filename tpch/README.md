## 🔁 OSS vs Turbo: TPCH Q1 Benchmark

### 📦 Dataset Details

These tests were run on TPCH lineitem dataset.

### GKE Cluster Specs

- **1 head node** — 8 CPUs, 32 GB RAM  
- **4 worker nodes** — each with 32 CPUs, 128 GB RAM  

#### 📊 Benchmark Results: Turbo vs OSS 

To reproduce the numbers you can run `python tpch_q1.py --sf 1000`  (1000 for 1TB, 100 for 100GB).

| Size   | Ray Turbo (s) | Ray OSS (s) | 
|--------|--------|--------------------|
| 100 GB  | 61.80  | 141.68              |
| 1 TB  | 398.883  | 1001.37             |
