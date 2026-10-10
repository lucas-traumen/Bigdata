"""TN10 - Ap dung tong hop cho bai toan xuyen suot (cells 38-39 notebook).

Bai toan: voi moi endpoint, co bao nhieu request loi (ERROR) va latency
trung binh cua cac request loi do.
  A (ngay tho): groupByKey ca dong, loc sau khi gom nhom
  B (toi uu)  : loc ngay dau, reduceByKey voi cap (tong, so luong)
"""

from common import bench, get_base, new_spark, parse, print_header, run_in_group, save_results, stage_metrics

print_header("TN10 - PIPELINE TONG HOP (cells 38-39)")

spark, sc = new_spark("TN10-PipelineTongHop")
base, n, _ = get_base(sc)


def pipeline_ngay_tho():
    return sorted(base.map(parse)
                      .map(lambda r: (r[3], r))
                      .groupByKey()
                      .mapValues(lambda rs: [x[5] for x in rs if x[0] == "ERROR"])
                      .mapValues(lambda ls: (len(ls), round(sum(ls) / len(ls), 1)))
                      .collect())


def pipeline_toi_uu():
    return sorted(base.filter(lambda l: l.startswith("ERROR"))
                      .map(lambda l: l.split())
                      .map(lambda c: (c[3], (int(c[5][:-2]), 1)))
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                      .mapValues(lambda s: (s[1], round(s[0] / s[1], 1)))
                      .collect())


kq_A = run_in_group("tn10_A", pipeline_ngay_tho, sc); m_A = stage_metrics("tn10_A", sc)
kq_B = run_in_group("tn10_B", pipeline_toi_uu, sc);   m_B = stage_metrics("tn10_B", sc)
assert kq_A == kq_B
t_A, _ = bench(pipeline_ngay_tho)
t_B, _ = bench(pipeline_toi_uu)

results = {
    "ket_qua": kq_B, "A": m_A, "B": m_B, "A_giay": t_A, "B_giay": t_B,
    "tang_toc": round(t_A / t_B, 1)}
save_results("tn10_pipeline_tong_hop", results)

print("Ket qua hai pipeline giong nhau: pass")
print(f"{'endpoint':<14} {'so_loi':>8} {'latency_tb_ms':>14}")
for ep, v in kq_B:
    print(f"{ep:<14} {v[0]:>8} {v[1]:>14}")
print(f"A: shuffle_bytes={m_A['shuffle_write_bytes']} tasks={m_A['so_task']} giay={t_A}")
print(f"B: shuffle_bytes={m_B['shuffle_write_bytes']} tasks={m_B['so_task']} giay={t_B}")
print(f"Tang toc: {results['tang_toc']} lan")

base.unpersist()
spark.stop()
