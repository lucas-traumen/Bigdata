"""TN3 - Nguyen tac 2: loc TRUOC shuffle (cells 15-17 notebook).

Cung ket qua: so cap (endpoint, IP) khac nhau co phat sinh loi.
  A: map(parse) -> map((level, endpoint, ip)) -> distinct() -> filter(ERROR)
  B: map(parse) -> filter(ERROR) -> map((endpoint, ip)) -> distinct()
"""

from common import bench, get_base, new_spark, parse, print_header, run_in_group, save_results, stage_metrics

print_header("TN3 - LOC TRUOC SHUFFLE (cells 15-17)")

spark, sc = new_spark("TN3-LocTruocShuffle")
base, n, _ = get_base(sc)


def tn3_A():
    return (base.map(parse).map(lambda r: (r[0], r[3], r[1])).distinct()
                .filter(lambda t: t[0] == "ERROR").count())


def tn3_B():
    return (base.map(parse).filter(lambda r: r[0] == "ERROR")
                .map(lambda r: (r[3], r[1])).distinct().count())


kq_A = run_in_group("tn3_A", tn3_A, sc); m_A = stage_metrics("tn3_A", sc)
kq_B = run_in_group("tn3_B", tn3_B, sc); m_B = stage_metrics("tn3_B", sc)
assert kq_A == kq_B
t_A, _ = bench(tn3_A)
t_B, _ = bench(tn3_B)

results = {
    "ket_qua": kq_A, "A": m_A, "B": m_B, "A_giay": t_A, "B_giay": t_B,
    "giam_shuffle_bytes": round(m_A["shuffle_write_bytes"] / m_B["shuffle_write_bytes"], 1),
    "tang_toc": round(t_A / t_B, 1)}
save_results("tn3_loc_truoc_shuffle", results)

print(f"Ket qua (so cap khac nhau)         : {kq_A} (A == B: pass)")
print(f"A shuffle_write_bytes / tasks      : {m_A['shuffle_write_bytes']} / {m_A['so_task']}")
print(f"B shuffle_write_bytes / tasks      : {m_B['shuffle_write_bytes']} / {m_B['so_task']}")
print(f"A thoi gian (giay)                 : {t_A}")
print(f"B thoi gian (giay)                 : {t_B}")
print(f"Giam byte shuffle (lan)            : {results['giam_shuffle_bytes']}")
print(f"Tang toc (lan)                     : {results['tang_toc']}")

base.unpersist()
spark.stop()
