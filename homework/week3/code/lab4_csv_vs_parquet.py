"""Lab 4: CSV vs Parquet experiment, 200,000 rows x 5 columns (slide 121-123).

Generates the transaction DataFrame exactly as slide 122 (same seed 42),
writes bench.csv and bench.parquet (snappy) into bench/, then compares
file sizes and the time to read ONE column (gia_tri).
Measurement note: the slide times a single read per format. Here each read
is repeated 3 times and the minimum is reported, to reduce OS cache /
scheduling noise; all individual runs are printed as well.
"""

import os
import time

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BENCH_DIR = os.path.join(BASE_DIR, "bench")


def timed(fn, *args, n_repeat=3, **kwargs):
    """Run fn(*args, **kwargs) n_repeat times, return list of elapsed seconds."""
    times = []
    for _ in range(n_repeat):
        t0 = time.time()
        fn(*args, **kwargs)
        times.append(time.time() - t0)
    return times


def fmt_times(times):
    return ", ".join(f"{t:.4f}" for t in times)


def main():
    os.makedirs(BENCH_DIR, exist_ok=True)

    np.random.seed(42)
    n = 200_000
    df = pd.DataFrame({
        "id": np.arange(n),
        "khu_vuc": np.random.choice(["HCM", "HN", "DN", "CT", "HP"], n),
        "gia_tri": np.random.exponential(50000, n).round(2),
        "so_luong": np.random.randint(1, 20, n),
        "ngay": pd.to_datetime("2026-01-01")
                + pd.to_timedelta(np.random.randint(0, 240, n), unit="D"),
    })
    csv_path = os.path.join(BENCH_DIR, "bench.csv")
    pq_path = os.path.join(BENCH_DIR, "bench.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(pq_path, engine="pyarrow", compression="snappy")

    csv_mb = os.path.getsize(csv_path) / 1e6
    pq_mb = os.path.getsize(pq_path) / 1e6
    print(f"DataFrame: {n} rows x {len(df.columns)} columns "
          f"({', '.join(df.columns)})")
    print(f"CSV size    : {csv_mb:.2f} MB")
    print(f"Parquet size: {pq_mb:.2f} MB (snappy)")
    print(f"Parquet = {pq_mb / csv_mb * 100:.1f}% of CSV size "
          f"(slide expectation: ~15-30%)")
    print()

    t_csv = timed(pd.read_csv, csv_path, usecols=["gia_tri"])
    t_pq = timed(pd.read_parquet, pq_path, columns=["gia_tri"])
    print("Read only column 'gia_tri' (3 runs each; slide reads once):")
    print(f"  CSV    : best {min(t_csv):.4f} s (runs: {fmt_times(t_csv)})")
    print(f"  Parquet: best {min(t_pq):.4f} s (runs: {fmt_times(t_pq)})")
    if min(t_pq) > 0:
        print(f"  speedup (CSV best / Parquet best) = {min(t_csv) / min(t_pq):.1f}x")
    print()
    print("Why: pd.read_csv must PARSE all 5 columns even with usecols, while")
    print("Parquet is columnar - read_parquet(columns=...) fetches exactly the")
    print("requested column group (column pruning), plus Snappy-compressed data.")
    print("Exact numbers depend on the machine (page cache, disk, CPU).")


if __name__ == "__main__":
    main()
