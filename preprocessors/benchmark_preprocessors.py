"""
Usage Examples:

# Benchmark for Preprocessors

# high cardinality column CUSTKEY

# CUSTKEY is int
# Preprocessors run: ["OrdinalEncoder['CUSTKEY']", "OrdinalEncoder['CLERK']", "OrdinalEncoder['ORDERDATE']"]
python lazy_test.py --size 1

# CUSTKEY is str
# Preprocessors run: ["OrdinalEncoder['CUSTKEY']", "OrdinalEncoder['CLERK']", "OrdinalEncoder['ORDERDATE']"]
python lazy_test.py --str --size 1
"""
import logging
import os
import time
from datetime import datetime

import ray
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.preprocessors import Chain, OrdinalEncoder, OneHotEncoder, \
    MultiHotEncoder, LabelEncoder, Categorizer, SimpleImputer, StandardScaler
import argparse

logger = logging.getLogger(__name__)


class ColumnMapper:
    """Simple column mapper that handles original column names automatically."""

    def __init__(self, logical_to_original_mapping: dict):
        """
        Initialize with a mapping from logical names to original column names.

        Args:
            logical_to_original_mapping: Dictionary mapping logical -> original column names
        """
        self.logical_to_original = logical_to_original_mapping

    def get(self, col_name: str) -> str:
        """
        Get the correct column name for use in the DataFrame.

        If col_name starts with "column", return as-is (it's already an original name).
        Otherwise, convert from logical name to original name.
        """
        if col_name.startswith("column"):
            return col_name
        return self.logical_to_original.get(col_name, col_name)

    def get_columns(self, col_names: list) -> list:
        """
        Get the correct column names for a list of columns.

        Args:
            col_names: List of column names (logical or original)

        Returns:
            List of original column names
        """
        return [self.get(col) for col in col_names]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPCH Q1")
    parser.add_argument("--size", choices=[1, 10, 100, 1000, 10000], type=int,
                        default=1)
    parser.add_argument("--enable_hash_shuffle", action="store_true")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--str", action="store_true")  # convert CUSTKEY to str
    parser.add_argument("--include_hot_pp", action="store_true", default=False)
    parser.add_argument("--include_hot_pp_with_max", action="store_true",
                        default=False)
    parser.add_argument("--include_label_pp", action="store_true",
                        default=False)
    parser.add_argument("--include_categorizer", action="store_true",
                        default=False)
    parser.add_argument("--include_imputer", action="store_true",
                        default=False)
    parser.add_argument("--include_scaler", action="store_true", default=False)
    parser.add_argument("--storage", type=str, default="gs", choices=["s3", "local", "gs"])
    return parser.parse_args()


def main(args):
    if args.storage == "local":
        ray.init(num_cpus=1)

    if args.str and (args.include_imputer or args.include_scaler):
        raise ValueError(
            "--str and --include_imputer / --include_scaler are mutually exclusive")

    start = time.perf_counter()

    dataset_path = f"{args.storage}://ray-benchmark-data/tpch/parquet/sf{args.size}/orders"
    if args.enable_hash_shuffle:
        DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
        # DataContext.get_current().default_hash_shuffle_parallelism = 64

    print("Start of execution")

    df = ray.data.read_parquet(dataset_path)
    if args.sample:
        df = df.random_sample(0.00003)

    # Define column mapping (logical -> original names)
    column_mapping = {
        "ORDERKEY": "column0",
        "CUSTKEY": "column1",
        "ORDERSTATUS": "column2",
        "TOTALPRICE": "column3",
        "ORDERDATE": "column4",
        "ORDER-PRIORITY": "column5",
        "CLERK": "column6",
        "SHIP-PRIORITY": "column7",
        "COMMENT": "column8"
    }

    # Create column mapper
    column_mapper = ColumnMapper(column_mapping)

    # Note: We don't use df.rename_columns() due to bugs
    # Instead, we work with original column names throughout

    # if args.include_imputer:
    #     df = df.map_batches(
    #         lambda batch: batch.assign(
    #             CUSTKEY=batch["CUSTKEY"].mask(batch.index % 5 == 0, np.nan)
    #         ),
    #         batch_format="pandas"
    #     )

    if args.str:
        df = df.map_batches(
            lambda batch: batch.assign(
                CUSTKEY=batch[column_mapper.get("CUSTKEY")].astype(str),
            ),
            batch_format="pandas"
        )

    # df = df.materialize()
    # df.select_columns(cols=['ORDERKEY', 'CUSTKEY']).sort(key='ORDERKEY').show()

    read_complete = time.perf_counter()

    print("-" * 30)
    print("Input is ready")

    # Use column mapper to get the correct column names
    custkey_col = column_mapper.get("CUSTKEY")  # "column1"
    clerk_col = column_mapper.get("CLERK")  # "column6"
    orderdate_col = column_mapper.get("ORDERDATE")  # "column4"
    orderkey_col = column_mapper.get("ORDERKEY")  # "column0"

    cols = [
        orderkey_col,
        custkey_col,
        'OrdinalEncoder_CUSTKEY',
        'OrdinalEncoder_CLERK',
        'OrdinalEncoder_ORDERDATE',
    ]

    preprocessors = [

        OrdinalEncoder(columns=[custkey_col],
                       output_columns=["OrdinalEncoder_CUSTKEY"]),

        OrdinalEncoder(columns=[clerk_col],
                       output_columns=["OrdinalEncoder_CLERK"]),

        OrdinalEncoder(columns=[orderdate_col],
                       output_columns=["OrdinalEncoder_ORDERDATE"]),
    ]

    if args.include_scaler:
        preprocessors.append(
            StandardScaler(columns=[custkey_col],
                           output_columns=["StandardScaler_CUSTKEY"]),
        )
        cols.append('StandardScaler_CUSTKEY')

    if args.include_imputer:
        preprocessors.extend([
            SimpleImputer(columns=[custkey_col],
                          # strategy="most_frequent",
                          output_columns=["SimpleImputer_CUSTKEY"]),
        ])
        cols.append("SimpleImputer_CUSTKEY")
    if args.include_hot_pp:
        preprocessors.extend([
            OneHotEncoder(columns=[orderdate_col],
                          output_columns=["OneHotEncoder_ORDERDATE"]),

            MultiHotEncoder(columns=[orderdate_col],
                            output_columns=["MultiHotEncoder_ORDERDATE"]),
        ])
        cols.extend([
            'OneHotEncoder_ORDERDATE',
            'MultiHotEncoder_ORDERDATE',
        ])
    if args.include_hot_pp_with_max:
        preprocessors.extend([
            OneHotEncoder(columns=[custkey_col],
                          max_categories={custkey_col: 10},
                          output_columns=["OneHotEncoder_with_max_CUSTKEY"]),

            MultiHotEncoder(columns=[custkey_col],
                            max_categories={custkey_col: 5},
                            output_columns=[
                                "MultiHotEncoder_with_max_CUSTKEY"]),
        ])
        cols.extend(['OneHotEncoder_with_max_CUSTKEY',
                     'MultiHotEncoder_with_max_CUSTKEY'])
    if args.include_label_pp:
        preprocessors.extend([
            LabelEncoder(label_column=custkey_col,
                         output_column="LabelEncoder_CUSTKEY"),
        ])
        cols.extend(['LabelEncoder_CUSTKEY'])
    if args.include_categorizer:
        preprocessors.append(
            Categorizer(columns=[custkey_col],
                        output_columns=["Categorizer_CUSTKEY"]),
        )
        cols.extend(['Categorizer_CUSTKEY'])

    chain = Chain(*preprocessors)
    chain.fit(df)

    if args.size >= 1000:
        df = chain.transform(df, concurrency=50, num_cpus=12)
    else:
        df = chain.transform(df)

    fit_complete = time.perf_counter()

    print("-" * 30)
    print(
        f'Preprocessors run: {[p.__class__.__name__ + str(column_mapper.get_columns(p.columns)) for p in preprocessors]}')
    print(f"Elapsed time (read): {read_complete - start:.6f} seconds")
    print(f"Elapsed time (fit): {fit_complete - read_complete:.6f} seconds")
    print("-" * 30)

    if args.storage == "local":
        df.select_columns(cols=cols).sort(key=orderkey_col).show()
        # df.write_parquet(f'/tmp/runs/{datetime.now().strftime("%Y%m%d_%H%M%S")}/')
    elif args.storage == "s3":
        df.write_parquet(
            f's3://cem-preprocessor/runs/{datetime.now().strftime("%Y%m%d_%H%M%S")}/')
    elif args.storage == "gs":
        df.write_parquet(
            f'{os.environ["ANYSCALE_ARTIFACT_STORAGE"]}/cem-benchmark-runs/{datetime.now().strftime("%Y%m%d_%H%M%S")}/')
    else:
        raise ValueError("Invalid storage type")

    transform_complete = time.perf_counter()

    print("-" * 30)
    print(
        f'Preprocessors run: {[p.__class__.__name__ + str(column_mapper.get_columns(p.columns)) for p in preprocessors]}')
    print(f"Elapsed time (read): {read_complete - start:.6f} seconds")
    print(f"Elapsed time (fit): {fit_complete - read_complete:.6f} seconds")
    print(
        f"Elapsed time (transform): {transform_complete - fit_complete:.6f} seconds")
    print(f"Elapsed time (total): {transform_complete - start:.6f} seconds")
    print("-" * 30)


if __name__ == "__main__":
    args = parse_args()
    main(args)
