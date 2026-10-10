"""TN5 - Nguyen tac 4: khong keo du lieu thua ve Driver (cells 21-23 notebook).

Cung ket qua: danh sach cac request loi 5xx.
  A: map(parse) -> collect() -> loc bang list comprehension tren Driver
  B: map(parse) -> filter(status >= 500) -> collect()
"""

from common import bench, get_base, new_spark, parse, print_header, save_results

print_header("TN5 - KHONG COLLECT THUA (cells 21-23)")

spark, sc = new_spark("TN5-KhongCollectThua")
base, n, _ = get_base(sc)


def tn5_A():
    return [r for r in base.map(parse).collect() if r[4] >= 500]


def tn5_B():
    return base.map(parse).filter(lambda r: r[4] >= 500).collect()


t_A, kq_A = bench(tn5_A)
t_B, kq_B = bench(tn5_B)
assert sorted(kq_A) == sorted(kq_B)

results = {
    "A_so_dong_ve_driver": n, "B_so_dong_ve_driver": len(kq_B),
    "A_giay": t_A, "B_giay": t_B,
    "giam_du_lieu_ve_driver": round(n / len(kq_B), 1), "tang_toc": round(t_A / t_B, 1)}
save_results("tn5_khong_collect_thua", results)

print(f"Ket qua hai cach giong nhau: pass")
print(f"A so dong ve Driver         : {n}")
print(f"B so dong ve Driver         : {len(kq_B)}")
print(f"A thoi gian (giay)          : {t_A}")
print(f"B thoi gian (giay)          : {t_B}")
print(f"Giam du lieu ve Driver (lan): {results['giam_du_lieu_ve_driver']}")
print(f"Tang toc (lan)              : {results['tang_toc']}")
del kq_A, kq_B

base.unpersist()
spark.stop()
