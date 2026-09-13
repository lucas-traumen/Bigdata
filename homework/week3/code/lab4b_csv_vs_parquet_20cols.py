"""Homework (slide 133): repeat Lab 4 with 20 columns, read only 2 columns.

Same transaction data as Lab 4 (id, khu_vuc, gia_tri, so_luong, ngay) plus
15 extra numeric columns (cot_01..cot_15, student-chosen ASCII names).
Goal: observe that the relative I/O saving of column pruning grows when the
table is wider - the theory example in the lecture says ~10x with 20 columns.
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
    # Same first 5 columns as Lab 4 (same seed and generation order).
    data = {
        "id": np.arange(n),
        "khu_vuc": np.random.choice(["HCM", "HN", "DN", "CT", "HP"], n),
        "gia_tri": np.random.exponential(50000, n).round(2),
        "so_luong": np.random.randint(1, 20, n),
        "ngay": pd.to_datetime("2026-01-01")
                + pd.to_timedelta(np.random.randint(0, 240, n), unit="D"),
    }
    # 15 extra numeric columns (ASCII names), 20 columns in total.
    for i in range(1, 16):
        data[f"cot_{i:02d}"] = np.random.exponential(1000, n).round(3)
    df = pd.DataFrame(data)

    csv_path = os.path.join(BENCH_DIR, "bench20.csv")
    pq_path = os.path.join(BENCH_DIR, "bench20.parquet")
    df.to_csv(csv_path, index=False)
    df.to_parquet(pq_path, engine="pyarrow", compression="snappy")

    csv_mb = os.path.getsize(csv_path) / 1e6
    pq_mb = os.path.getsize(pq_path) / 1e6
    print(f"DataFrame: {n} rows x {len(df.columns)} columns")
    print(f"Columns: {', '.join(df.columns)}")
    print(f"CSV size    : {csv_mb:.2f} MB")
    print(f"Parquet size: {pq_mb:.2f} MB (snappy)")
    print(f"Parquet = {pq_mb / csv_mb * 100:.1f}% of CSV size")
    print()

    t_csv = timed(pd.read_csv, csv_path, usecols=["gia_tri", "so_luong"])
    t_pq = timed(pd.read_parquet, pq_path, columns=["gia_tri", "so_luong"])
    print("Read only 2 columns ('gia_tri', 'so_luong') - 3 runs each:")
    print(f"  CSV    : best {min(t_csv):.4f} s (runs: {fmt_times(t_csv)})")
    print(f"  Parquet: best {min(t_pq):.4f} s (runs: {fmt_times(t_pq)})")
    if min(t_pq) > 0:
        print(f"  speedup (CSV best / Parquet best) = {min(t_csv) / min(t_pq):.1f}x")
    print()
    print("Analysis: 2 of 20 columns = 10% of the row width. The theory example")
    print("predicts ~10x less I/O with column pruning on a wide table. CSV gains")
    print("almost nothing from usecols (it still parses all 20 fields per row),")
    print("while Parquet skips the other 18 column chunks entirely - so the")
    print("speedup here should be clearly larger than in the 5-column Lab 4.")


if __name__ == "__main__":
    main()
