import time
import ray
from ray.data._internal.execution.util import memory_string

DATASET_URI = "path/to/your/s3"

def benchmark(ds_factory):
    start_time = time.time()

    ds = ds_factory()

    nbytes = 0 
    time_to_first_bundle = None
    for bundle in ds.iter_internal_ref_bundles():
        nbytes += bundle.size_bytes()
        if time_to_first_bundle is None:
            time_to_first_bundle = time.time() - start_time

    print(f"Read {memory_string(nbytes)} in {time.time() - start_time:.02f}s")
    print(f"First bundle in {time_to_first_bundle:.02f}s")


# S3 scales to high request rates. To ensure that the second method called doesn't have 
# a performance advantage, I'm doing a "warmup".
benchmark(lambda: ray.data.read_parquet(DATASET_URI))

# Benchmark runtime
benchmark(lambda: ray.data.read_parquet(DATASET_URI)) 
