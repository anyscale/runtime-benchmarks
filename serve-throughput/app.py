from fastapi import FastAPI
import torch
from ray import serve
import numpy as np
from model import build_model_from_args
from config import config

import logging
from itertools import chain
logger = logging.getLogger("ray.serve")

@serve.deployment(
    name="ranker_deployment",
    num_replicas=1,
    ray_actor_options={"num_gpus":1},
    max_ongoing_requests=1000,
)
class RankerDeployment:
    def __init__(self):
        self.model, self.device = build_model_from_args(config)
        # Pre-allocate a dummy tensor to force weights onto device
        _ = self.model(
            torch.zeros((1, config.num_dense_features), device=self.device),
            torch.zeros((1, config.num_sparse_features), dtype=torch.long, device=self.device),
        )
        logger.info(f"Device is {self.device}")

    @serve.batch(max_batch_size=100, batch_wait_timeout_s=0.05)
    async def rank(self, payloads: list):
        # payloads is a list of dicts, each containing "dense" and "sparse"
        # Flatten all samples from all payloads into a single batch
        logger.info(f"Batch size is {len(payloads)}")
        batch_sizes = [len(p["dense"]) for p in payloads]
        all_dense = list(chain.from_iterable(p["dense"] for p in payloads))
        all_sparse = list(chain.from_iterable(p["sparse"] for p in payloads))
 
        # Convert to tensors
        dense_np = np.asarray(all_dense, dtype=np.float32)
        sparse_np = np.asarray(all_sparse, dtype=np.int64)

        dense = torch.from_numpy(dense_np).to(self.device, non_blocking=True)
        sparse = torch.from_numpy(sparse_np).to(self.device, non_blocking=True)
        
        with torch.inference_mode():
            y = self.model(dense, sparse)
        
        # Split results back into separate responses for each request
        chunks = y.split(batch_sizes, dim=0)

        results = [{"scores": chunk.detach().cpu().tolist()} for chunk in chunks]
        return results

app = FastAPI()

@serve.deployment(
    name="ingress_deployment",
    num_replicas=2,
    max_ongoing_requests=1000,
)
@serve.ingress(app)
class IngressDeployment:
    def __init__(self, handle):
        self.handle = handle

    def synth_batch(self, config):
        # generate B candidates
        B = 32
        dense = np.random.random((B, config.num_dense_features)).astype("float32")
        sparse = np.random.randint(
            low=0, high=config.cardinality,
            size=(B, config.num_sparse_features),
            dtype="int64",
        )
        return {"dense": dense.tolist(), "sparse": sparse.tolist()}

    @app.get("/")
    async def infer(self, user_id: int):
        return await self.handle.rank.remote(self.synth_batch(config))

recsys_app = IngressDeployment.bind(
    RankerDeployment.bind()
)
