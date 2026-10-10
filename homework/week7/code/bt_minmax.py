"""BT_VE_NHA - Tinh min/max latency theo endpoint (bt_minmax.py).

1. Cach chinh: reduceByKey gop bo (min, max, tong, dem) - min/max co tinh
   ket hop + giao hoan nen an toan (khac phep chia/trung binh TN9).
2. Kiem chung tinh dung dan: chay lai voi 1/4/8/32 partition - ket qua
   phai dong nhat.
3. Doi chieu doc lap: pandas doc thang data/access.log, groupby endpoint.
4. So sanh cach ngay tho: collect() toan bo parse ve Driver roi min/max
   bang Python thuan - do thoi gian va so dong ve Driver (nguyen tac NT4).
"""

import time

from common import bench, get_base, new_spark, parse, print_header, save_results

print_header("BT VE NHA - MIN/MAX LATENCY THEO ENDPOINT (bt_minmax)")

spark, sc = new_spark("BT-MinMax")
base, n, _ = get_base(sc)

# ---------- 1. Cach chinh: reduceByKey (min, max, tong, dem) ----------
print("\n--- Cach chinh: reduceByKey gop (min, max, tong, dem) ---")


def minmax_rdd(num_parts=None):
    rdd = base.map(parse)
    if num_parts is not None:
        rdd = rdd.repartition(num_parts)
    return (rdd.map(lambda r: (r[3], (r[5], r[5], r[5], 1)))
               .reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1]),
                                          a[2] + b[2], a[3] + b[3]))
               .mapValues(lambda s: (s[0], s[1], round(s[2] / s[3], 2)))
               .collect())


kq_chinh = sorted(minmax_rdd())
print(f"{'endpoint':<14} {'min_ms':>7} {'max_ms':>7} {'mean_ms':>8}")
for ep, (mn, mx, mean) in kq_chinh:
    print(f"{ep:<14} {mn:>7} {mx:>7} {mean:>8}")

# ---------- 2. Kiem chung voi cac so partition khac nhau ----------
print("\n--- Kiem chung dong nhat voi 1/4/8/32 partition ---")
kq_part = {}
for p in (1, 4, 8, 32):
    kq_part[p] = sorted(minmax_rdd(p))
    assert kq_part[p] == kq_chinh, f"khac nhau tai partition={p}"
    print(f"  partition {p:>2}: giong cach chinh -> pass")

# ---------- 3. Doi chieu doc lap bang pandas ----------
print("\n--- Doi chieu doc lap: pandas groupby ---")
import pandas as pd

# Mo phung dung phep nhan ban x200 nhu notebook (latency +/-20ms, octet IP +37k)
# de doi chieu tren cung 1.2 trieu dong
from common import SCALE, nhan_ban
rows = []
for line in open("data/access.log"):
    rows.extend({"endpoint": c[3], "latency": int(c[5][:-2])}
                for c in (r.split() for r in nhan_ban(line)))
df = pd.DataFrame(rows)
assert len(df) == n
pandas_kq = sorted((ep, (int(g.min()), int(g.max()), round(g.mean(), 2)))
                   for ep, g in df.groupby("endpoint")["latency"])
assert pandas_kq == kq_chinh, f"pandas khac spark: {pandas_kq} vs {kq_chinh}"
print("  pandas groupby == ket qua Spark -> pass (6 endpoint khop tung gia tri)")

# ---------- 4. So sanh cach ngay tho: collect() ve Driver ----------
print("\n--- So sanh cach ngay tho: collect() 1.2 trieu dong ve Driver ---")


def ngay_tho():
    rows_sp = base.map(parse).collect()   # 1.2 trieu dong ve Driver
    per = {}
    for r in rows_sp:
        cur = per.get(r[3])
        if cur is None:
            per[r[3]] = [r[5], r[5], r[5], 1]
        else:
            cur[0] = min(cur[0], r[5]); cur[1] = max(cur[1], r[5])
            cur[2] += r[5]; cur[3] += 1
    return sorted((ep, (v[0], v[1], round(v[2] / v[3], 2))) for ep, v in per.items())


t0 = time.perf_counter()
kq_nt = ngay_tho()
t_nt = round(time.perf_counter() - t0, 3)
assert kq_nt == kq_chinh
so_dong_collect = n   # moi lan collect() keo 1.2 trieu dong ve Driver

t_chinh, _ = bench(lambda: minmax_rdd())

results = {
    "bang_endpoint": [{"endpoint": ep, "min_ms": mn, "max_ms": mx, "mean_ms": mean}
                      for ep, (mn, mx, mean) in kq_chinh],
    "kiem_chung_partition": {"cac_muc": [1, 4, 8, 32], "dong_nhat": True},
    "khop_pandas": True,
    "cach_chinh_giay": t_chinh,
    "ngay_tho_giay": t_nt,
    "so_dong_collect_ve_driver": so_dong_collect,
    "so_dong_ket_qua_ve_driver": len(kq_chinh),
    "tang_toc": round(t_nt / t_chinh, 1)}
save_results("bt_minmax", results)

print(f"Cach ngay tho  : {t_nt}s (keo {so_dong_collect:,} dong ve Driver)")
print(f"Cach reduceByKey: {t_chinh}s (chi {len(kq_chinh)} dong ve Driver)")
print(f"Tang toc       : {results['tang_toc']} lan")

base.unpersist()
spark.stop()
