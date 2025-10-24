## 🔁 OSS vs Turbo: Ray Serve Throughput Benchmark

### 📦 Benchmark detail

This benchmark runs a recommendation model with 2 Ray Serve deployments - the Ingress Deployment that receives the request and generates candidates, the Ranker Deployment that runs a Deep Learning Recommendation Model on a GPU. The throughput measurement is run using locust - locust file is provided.

### Cluster Specs

- **1 head node** — 8 CPUs, 32 GB RAM  
- **Worker Group 1: 1 g6e.12xlarge nodes (g2-standard-48 in GCP)**

### 📊 Benchmark Results: Turbo vs OSS 

To reproduce the numbers you need to:
1. Build an image with the python requirements and environment variables - The Dockerfile is provided that works with both RayTurbo and OSS Ray.
2. Deploy a service through KubeRay or Anyscale (eg. `anyscale service deploy app:recsys_app --name my-test-service`). 
2. Run locust on your client side. Make sure that your client node has sufficient CPUs and you run enough locust users to saturate the throughput. Example locust command - `locust --headless --host <HOST_URL> -r 800 -u 800 --proc 30 -t 2m`

Keep a 1:2 ratio between replicas of the Ranker and Ingress Deployment

| Number of GPU replicas | Ray Turbo (req/s) | Ray OSS (req/s) | Latency (p50) Ray Turbo | Latency (p50) OSS | Latency (p90) Ray Turbo | Latency (p90) OSS | Locust users RT | Locust users OSS | 
|--------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|--------------------|
| 1  | 1920             | 570              | 100              | 130              | 120              | 190              | 200              | 75              | 
| 2  | 3540              | 850              | 110              | 110              | 150              | 180              | 400              | 100              | 
| 4  | 6300              | 900              | 110              |  120             |  180             | 190              | 750             | 100              | 







