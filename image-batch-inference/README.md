## 🔁 OSS vs Anyscale Runtime: Image Inference benchmark

### 📦 Dataset Details

This benchmark is run on 10TB of images in JSONL format.

### Cluster Specs

- **1 head node** — 8 CPUs, 32 GB RAM  
- **Worker Group 1: 40 g2-standard-16 nodes**
- **Worker Group 2: 101 n2-standard-16 nodes**

### 📊 Benchmark Results: Anyscale Runtime vs OSS 

To reproduce the numbers you can run `python batch_inference.py`.

| Anyscale Runtime (s) | Ray OSS (s) | 
|--------|--------------------|
| 431  | 772              |


### Running on GKE with Kuberay

Steps:

1. Create a GKE cluster with the following nodes:
  - 40 g2-standard-16 nodes with label `gce-node-type: g2-standard-16`
  - 101 n2-standard-16 nodes with label `gce-node-type: n2-standard-16`
2. Update the `rayClusterSpec` in `job-gke.yaml` to use the appropriate images
3. Update `OUTPUT_PREFIX` in `batch_inference.py` to point to a writeable bucket
4. Run `kubectl apply -f job-gke.yaml`

