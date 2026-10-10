"""TN4 - Nguyen tac 3: bot cot va gop cuc bo truoc shuffle (cells 18-20 notebook).

Cung ket qua: tong latency theo endpoint. Ba cach, moi buoc doi MOT yeu to:
  A: ca dong (6 truong) + groupByKey
  B: chi (endpoint, latency) + groupByKey
  C: chi (endpoint, latency) + reduceByKey
"""

from operator import add

from common import bench, get_base, new_spark, parse, print_header, run_in_group, save_results, stage_metrics

print_header("TN4 - BOT COT + GOP CUC BO (cells 18-20)")

spark, sc = new_spark("TN4-BotCotGopCucBo")
base, n, _ = get_base(sc)


def tn4_A():
    return sorted(base.map(parse).map(lambda r: (r[3], r)).groupByKey()
                      .mapValues(lambda rs: sum(x[5] for x in rs)).collect())


def tn4_B():
    return sorted(base.map(parse).map(lambda r: (r[3], r[5])).groupByKey()
                      .mapValues(sum).collect())


def tn4_C():
    return sorted(base.map(parse).map(lambda r: (r[3], r[5]))
                      .reduceByKey(add).collect())


out = {}
for name, fn in [("A", tn4_A), ("B", tn4_B), ("C", tn4_C)]:
    kq = run_in_group(f"tn4_{name}", fn, sc)
    out[name] = {"ket_qua": kq, **stage_metrics(f"tn4_{name}", sc)}
assert out["A"]["ket_qua"] == out["B"]["ket_qua"] == out["C"]["ket_qua"]
for name, fn in [("A", tn4_A), ("B", tn4_B), ("C", tn4_C)]:
    out[name]["giay"], _ = bench(fn)
    out[name].pop("ket_qua")

save_results("tn4_bot_cot_gop_cuc_bo", out)

print(f"Ket qua 3 cach giong nhau: pass (tong latency theo endpoint)")
for k in ("A", "B", "C"):
    v = out[k]
    print(f"  {k}: shuffle_bytes={v['shuffle_write_bytes']:<10} "
          f"shuffle_records={v['shuffle_write_records']:<8} "
          f"tasks={v['so_task']:<4} thoi_gian={v['giay']}s")
print(f"A->B giam byte (chieu cot) : {round(out['A']['shuffle_write_bytes'] / out['B']['shuffle_write_bytes'], 1)} lan")
print(f"B->C giam byte (gop cuc bo): {round(out['B']['shuffle_write_bytes'] / out['C']['shuffle_write_bytes'], 1)} lan")

base.unpersist()
spark.stop()
