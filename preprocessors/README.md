
## 🔁 OSS vs Turbo: OrdinalEncoder Benchmark

### ✅ Machine Specs

Cluster configuration used for this benchmark:

- **1 head node** — 8 CPUs, 32 GB RAM  
- **25 worker nodes** — each with 32 CPUs, 128 GB RAM  
- **Total**: **808 CPUs**, **3.2 TB RAM**

---

### 📦 Dataset Characteristics (High-Cardinality `CUSTKEY` Column)

| Size   | Rows  | Unique `CUSTKEY` Values |
|--------|-------|-------------------------|
| 1 GB   | 1.5M  | 99,996                  |
| 10 GB  | 15M   | 999,962                 |
| 100 GB | 150M  | 9,999,832               |
| 1 TB   | 1.5B  | 99,999,998              |

---

### ⚙️ Execution Notes

- **Script invocation**:
  ```bash
  python lazy_test.py --size <size> --storage gs
  ```

- **Preprocessors**:
  - `OrdinalEncoder['CUSTKEY']`
  - `OrdinalEncoder['CLERK']`
  - `OrdinalEncoder['ORDERDATE']`

---

### 📊 Benchmark Results: Turbo vs OSS (`OrdinalEncoder[CUSTKEY/CLERK/ORDERDATE]`)

| Size   | Engine | Read (s) | Fit (s) | Transform (s) | Total (s) | Speedup vs OSS (×) |
|--------|--------|----------|---------|----------------|-----------|---------------------|
| 1 GB   | Turbo  | 0.54     | 4.71    | 17.38          | 22.64     | 20.1×               |
|        | OSS    | 2.33     | 282.05  | 170.54         | 454.93    | —                   |
| 10 GB  | Turbo  | 0.48     | 6.23    | 18.53          | 25.24     | 17.4×               |
|        | OSS    | 2.28     | 412.56  | 23.91          | 438.76    | —                   |
| 100 GB | Turbo  | 0.52     | 25.15   | 31.40          | 57.07     | 103.2×              |
|        | OSS    | 2.32     | 5823.84 | 64.49          | 5890.65   | —                   |
| 1 TB   | Turbo  | 0.56     | 339.59  | 2318.71        | 2658.85   | —                   |
|        | OSS    | ❌       | ❌      | ❌             | ❌        | —                   |

---

> ✅ **Turbo outperforms OSS by 17× to 103×** in total time across 1–100 GB datasets  
> ⚠️ OSS fails to complete at 1 TB scale  
> 📈 Turbo completes 1 TB run in ~45 minutes, demonstrating strong scalability
