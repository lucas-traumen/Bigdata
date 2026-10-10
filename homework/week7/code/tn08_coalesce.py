"""TN8 - Nguyen tac 7: coalesce() sau khi loc manh (cells 32-34 notebook).

Chia lai du lieu thanh 400 partition, loc giu ERROR + GET,
roi tinh tong latency theo endpoint. A: giu 400 partition; B: coalesce(8).
"""

from operator import add

from common import bench, get_base, new_spark, parse, print_header, run_in_group, save_results, stage_metrics

print_header("TN8 - COALESCE SAU LOC MANH (cells 32-34)")

spark, sc = new_spark("TN8-Coalesce")
base, n, _ = get_base(sc)

many = base.repartition(400).cache()
many.count()


def loc_manh(l):
    return l.startswith("ERROR") and " GET " in l


def tn8_A():
    return sorted(many.filter(loc_manh).map(parse).map(lambda r: (r[3], r[5]))
                      .reduceByKey(add).collect())


def tn8_B():
    return sorted(many.filter(loc_manh).coalesce(8).map(parse).map(lambda r: (r[3], r[5]))
                      .reduceByKey(add).collect())


kq_A = run_in_group("tn8_A", tn8_A, sc); m_A = stage_metrics("tn8_A", sc)
kq_B = run_in_group("tn8_B", tn8_B, sc); m_B = stage_metrics("tn8_B", sc)
assert kq_A == kq_B
t_A, _ = bench(tn8_A)
t_B, _ = bench(tn8_B)

results = {
    "so_dong_sau_loc": many.filter(loc_manh).count(),
    "A": m_A, "B": m_B, "A_giay": t_A, "B_giay": t_B, "tang_toc": round(t_A / t_B, 1)}
many.unpersist()
save_results("tn8_coalesce", results)

print(f"So dong sau loc (ERROR + GET)      : {results['so_dong_sau_loc']}")
print(f"A so task (400 partition)          : {m_A['so_task']}")
print(f"B so task (coalesce 8)             : {m_B['so_task']}")
print(f"A giay: {t_A} | B giay: {t_B} | Tang toc: {results['tang_toc']} lan")

base.unpersist()
spark.stop()
