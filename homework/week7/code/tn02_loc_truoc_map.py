"""TN2 - Nguyen tac 1: loc TRUOC phep bien doi dat tien (cells 12-14 notebook).

Cung ket qua: dem so dong ERROR.
  A (chua toi uu): map(parse) -> filter(level == "ERROR")
  B (toi uu):      filter(startswith("ERROR")) -> map(parse)
"""

from common import bench, counted, get_base, new_spark, parse, print_header, save_results

print_header("TN2 - LOC TRUOC MAP (cells 12-14)")

spark, sc = new_spark("TN2-LocTruocMap")
base, n, _ = get_base(sc)

a_A, a_B = sc.accumulator(0), sc.accumulator(0)
kq_A = base.map(counted(parse, a_A)).filter(lambda r: r[0] == "ERROR").count()
kq_B = base.filter(lambda l: l.startswith("ERROR")).map(counted(parse, a_B)).count()
assert kq_A == kq_B

t_A, _ = bench(lambda: base.map(parse).filter(lambda r: r[0] == "ERROR").count())
t_B, _ = bench(lambda: base.filter(lambda l: l.startswith("ERROR")).map(parse).count())

results = {
    "ket_qua": kq_A,
    "A_so_lan_parse": a_A.value, "B_so_lan_parse": a_B.value,
    "A_giay": t_A, "B_giay": t_B,
    "giam_so_lan_goi": round(a_A.value / a_B.value, 1), "tang_toc": round(t_A / t_B, 1)}
save_results("tn2_loc_truoc_map", results)

print(f"Ket qua dem dong ERROR : {kq_A} (A == B: pass)")
print(f"A so lan parse         : {a_A.value}")
print(f"B so lan parse         : {a_B.value}")
print(f"A thoi gian (giay)     : {t_A}")
print(f"B thoi gian (giay)     : {t_B}")
print(f"Giam so lan goi (lan)  : {results['giam_so_lan_goi']}")
print(f"Tang toc (lan)         : {results['tang_toc']}")

base.unpersist()
spark.stop()
