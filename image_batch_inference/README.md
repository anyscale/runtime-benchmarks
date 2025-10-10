Steps:
1. Create a GKE cluster with the following nodes:
  - 40 g2-standard-16 nodes with label `gce-node-type: g2-standard-16`
  - 101 n2-standard-16 nodes with label `gce-node-type: n2-standard-16`
2. Update the `rayClusterSpec` to use the appropriate images
3. Update `OUTPUT_PREFIX` in `batch_inference.py` to point to a writeable bucket
4. Run `kubectl apply -f job.yaml`

Results:
- RayTurbo (2.50rc): 07:11 
- OSS Ray (nightly): 12:52