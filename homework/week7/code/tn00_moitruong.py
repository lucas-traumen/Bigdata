"""TN0 - Chuan bi: cau hinh may do + nhan ban du lieu (cells 0-6 notebook)."""

import platform

from common import (cpu_name, get_base, get_raw, new_spark, print_header,
                    ram_gb, save_results, SCALE)

print_header("TN0 - MOI TRUONG VA DU LIEU (cells 0-6)")

spark, sc = new_spark("TN0-MoiTruong")

print(f"Spark version : {spark.version}")
print(f"Master        : {sc.master}")
print(f"So core       : {sc.defaultParallelism}")
print(f"Spark UI      : {sc.uiWebUrl}")

moi_truong = {
    "cpu": cpu_name(),
    "so_core_spark": sc.defaultParallelism,
    "ram_gb": ram_gb(),
    "he_dieu_hanh": platform.platform(),
    "python": platform.python_version(),
    "spark": spark.version,
    "master": sc.master,
}
save_results("moi_truong", moi_truong)
print("\nMOI TRUONG:")
for k, v in moi_truong.items():
    print(f"  {k:<16}: {v}")

raw = get_raw(sc)
base, n, _ = get_base(sc, raw)
du_lieu = {"so_dong_goc": raw.count(), "he_so_nhan": SCALE,
           "so_dong": n, "so_partition": base.getNumPartitions()}
save_results("du_lieu", du_lieu)
print("\nDU LIEU:")
for k, v in du_lieu.items():
    print(f"  {k:<14}: {v}")

base.unpersist()
spark.stop()
