import argparse
from datetime import datetime, timedelta
import pyarrow as pa
import time

import ray
from ray.data.expressions import udf, col, DataType

from ray.data.aggregate import Count, Mean, Sum


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPCH Q1")
    parser.add_argument("--sf", choices=[1, 10, 100, 1000, 10000], type=int, default=1)
    return parser.parse_args()


def core_warmup():

    @ray.remote
    def _noop():
        pass

    num_cpus = int(ray.available_resources()["CPU"])

    print(f">>> Warming up {num_cpus} workers")

    refs = [_noop.remote() for _ in range(num_cpus)]

    ray.get(refs)

@udf(return_dtype=DataType.float64())
def to_f64(arr: pa.Array) -> pa.Array:
    """Cast any numeric type to float64."""
    import pyarrow.compute as pc
    return pc.cast(arr, pa.float64())


def q1_fixed(path):
    ds = ray.data.read_parquet(path)
    cutoff = datetime(1998, 12, 1) - timedelta(days=90)
    ds = ds.filter(expr=col("column10") <= cutoff)

    # Build float views + derived columns
    ds = (
        ds.with_column("l_quantity_f", to_f64(col("column04")))
        .with_column("l_extendedprice_f", to_f64(col("column05")))
        .with_column("l_discount_f", to_f64(col("column06")))
        .with_column("l_tax_f", to_f64(col("column07")))
        .with_column(
            "disc_price",
            to_f64(col("column05")) * (1 - to_f64(col("column06"))),
            )
        .with_column("charge", col("disc_price") * (1 + to_f64(col("column07"))))
    )

    # Drop original DECIMALs
    ds = ds.select_columns(
        [
            "column08",
            "column09",
            "l_quantity_f",
            "l_extendedprice_f",
            "l_discount_f",
            "disc_price",
            "charge",
        ]
    )

    result = (
        ds.groupby(["column08", "column09"])
        .aggregate(
            Sum(on="l_quantity_f", alias_name="sum_qty"),
            Sum(on="l_extendedprice_f", alias_name="sum_base_price"),
            Sum(on="disc_price", alias_name="sum_disc_price"),
            Sum(on="charge", alias_name="sum_charge"),
            Mean(on="l_quantity_f", alias_name="avg_qty"),
            Mean(on="l_extendedprice_f", alias_name="avg_price"),
            Mean(on="l_discount_f", alias_name="avg_disc"),
            Count(alias_name="count_order"),
        )
        .sort(key=["column08", "column09"])
        .select_columns(
            [
                "column08",
                "column09",
                "sum_qty",
                "sum_base_price",
                "sum_disc_price",
                "sum_charge",
                "avg_qty",
                "avg_price",
                "avg_disc",
                "count_order",
            ]
        )
    )
    return result

def main(args):
    core_warmup()

    path = f"gs://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"

    start = time.perf_counter()

    q1_fixed(path).materialize()

    end_time = time.perf_counter()
    print(f"Elapsed time : {end_time - start:.6f} seconds")


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
