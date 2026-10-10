"""TN9 - Tinh dung dan: reduce voi ham khong ket hop (cells 35-37 notebook).

1. Phep chia tren [1, 2, 0.5, 0.1, 5, 0.2] voi 1, 2, 3, 6 partition.
2. "Trung binh" sai: reduce((x+y)/2) tren 6000 gia tri latency, so voi trung binh dung.
3. Cach dung: mang theo cap (tong, so luong), chi chia mot lan o cuoi.
"""

from common import get_raw, new_spark, parse, print_header, save_results

print_header("TN9 - TINH DUNG DAN CUA REDUCE (cells 35-37)")

spark, sc = new_spark("TN9-TinhDungDan")
raw = get_raw(sc)

so = [1, 2, .5, .1, 5, .2]
chia = {p: round(sc.parallelize(so, p).reduce(lambda x, y: x / y), 6) for p in (1, 2, 3, 6)}

lat = raw.map(lambda l: int(l.split()[5][:-2]))
tb_dung = lat.sum() / lat.count()
tb_sai = {p: round(lat.repartition(p).reduce(lambda x, y: (x + y) / 2), 2) for p in (1, 4, 8)}

tong, dem = lat.map(lambda x: (x, 1)).reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))

tb_endpoint = sorted(raw.map(parse)
                        .map(lambda r: (r[3], (r[5], 1)))
                        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                        .mapValues(lambda s: round(s[0] / s[1], 1))
                        .collect())

results = {
    "reduce_chia_theo_so_partition": chia,
    "trung_binh_dung": round(tb_dung, 2),
    "trung_binh_sai_theo_so_partition": tb_sai,
    "trung_binh_tong_dem": round(tong / dem, 2),
    "trung_binh_theo_endpoint": tb_endpoint}
save_results("tn9_tinh_dung_dan", results)

print("Phep chia theo so partition (khong ket hop -> ket qua khac nhau):")
for p, v in chia.items():
    print(f"  {p} partition -> {v}")
print(f"\nTrung binh DUNG (sum/count)           : {results['trung_binh_dung']}")
print("Trung binh SAI ((x+y)/2) theo partition:")
for p, v in tb_sai.items():
    print(f"  {p} partition -> {v}")
print(f"Trung binh cach dung (tong, dem)      : {results['trung_binh_tong_dem']}")
print("Trung binh latency theo endpoint (cach dung):")
for ep, v in tb_endpoint:
    print(f"  {ep:<14}: {v} ms")

spark.stop()
