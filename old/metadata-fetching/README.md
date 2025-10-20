# metadata-fetching-benchmarks
Benchmark script for reproducing metadata fetching blog numbers.

To reproduce the numbers:
1. You can run `create_data.py` to create a 1 TiB dataset
2. Use `reproduce.py` to read from the dataset.

The blog uses 1 m5.2xlarge head node, and 8 m5.8xlarge worker nodes for the underlying Ray cluster.
