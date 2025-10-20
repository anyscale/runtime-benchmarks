import random

import ray

NUM_BYTES_PER_VALUE = 8  # 8 bytes for an int64
MAX_VALUE = 2**31 - 1
NUM_COLUMNS = 16
NUM_BYTES_PER_FILE = 128 * 1024**2  # 128 MiB
NUM_ROWS_PER_FILE = NUM_BYTES_PER_FILE // (NUM_COLUMNS * NUM_BYTES_PER_VALUE)
DATASET_SIZE = 1024**4  # 1 TiB
NUM_FILES = DATASET_SIZE // NUM_BYTES_PER_FILE

env_vars = dict(
    AWS_ACCESS_KEY_ID=...,
    AWS_SECRET_ACCESS_KEY=...
    AWS_SESSION_TOKEN=...
)

ray.init(runtime_env={"env_vars": env_vars})
import numpy as np


def generate_data(_):
    yield {
        f"column{i}": np.random.randint(2**63 - 1, size=(NUM_ROWS_PER_FILE,)) for i in range(NUM_COLUMNS)
    }


ray.data.DataContext.target_max_block_size = NUM_BYTES_PER_FILE
(
    ray.data.range(NUM_FILES, override_num_blocks=NUM_FILES)
    .map_batches(generate_data, batch_size=1)
    .write_parquet(
        "s3://ray-benchmark-data/parquet/128MiB-file/1TiB",
        concurrency=8,
        ray_remote_args={"scheduling_strategy": "SPREAD"}
    )
)

