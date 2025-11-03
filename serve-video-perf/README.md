# 🎥 Ray Serve Video Inference Benchmark

## 📋 Overview

This benchmark evaluates the performance of **Anyscale Runtime** vs **Open Source Ray (OSS)** for real-time video processing and object detection workloads. The benchmark demonstrates significant performance improvements in video inference pipelines using improved object transfer bandwidth on Anyscale Runtime.

## 🏗️ Architecture

The benchmark implements a **4-stage video processing pipeline** with specialized Ray Serve deployments:

### Pipeline Components

1. **`VideoProcessingAPI`** - FastAPI ingress that receives video processing requests
2. **`VideoProcessingService`** - Handles video chunking and orchestrates the entire video inference workflow
3. **`VideoDecoder`** - Handles frame extraction using FFmpeg
4. **`ModelInference`** - Runs object detection inference on extracted frames

### Processing Flow

```
Video Input → Chunking → Frame Extraction → Object Detection → Results
     ↓           ↓            ↓                ↓              ↓
   API Layer → Service → Decoder (FFmpeg) → GPU Inference → Response
```

Two inference scenarios are implemented: **chunked inference** (processing each chunk independently) and **combined inference** (processing all frames together for applications that require merging the entire video). This can be controlled through the inputs in `req.py`.
 
## ⚙️ Infrastructure Requirements

### Cluster Configuration

| Component | Specification | Purpose |
|-----------|---------------|---------|
| **Head Node** | 8 CPUs, 32 GB RAM (0 CPUs for scheduling) | Ray cluster coordination |
| **GPU Workers** | 1 × g5.24xlarge nodes | Model inference processing |
| **Chunk Workers** | 2 × m7i.16xlarge nodes (set custom resource ``"chunk_only": 1`) | Video chunking operations |
| **Decode Workers** | 1 × m7i.16xlarge node (set custom resource ``"decode_only": 1`) | Frame extraction processing |

## Running the Benchmark

### Installation

The Dockerfile is provided - Deploy without these 2 environment variables for the baseline:
ENV RAY_experimental_object_manager_enable_multiple_connections=1 
ENV RAY_object_manager_client_connection_num=15

```bash
docker build -t video-inference-benchmark .
```

### Execution

1. **Deploy the Serve Application**
   ```bash
   serve run app:api
   ```

2. **Execute Benchmark**
   ```bash
   python req.py
   ```

## 📊 Performance Results

| Scenario | Average latency - Anyscale Runtime | Average latency - OSS Ray | Improvement |
|----------|-----------|---------|-------------|
| **Chunked Inference** | 66s | 90s | **26% faster** |
| **Combined Inference** | 68s | 94s |  **27% faster** |






