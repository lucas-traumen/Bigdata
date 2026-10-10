"""TN1 - Lazy evaluation va cache(): dem so lan goi parse khi count() hai lan
(cells 9-11 notebook). Dung file goc 6000 dong (raw), KHONG phai base."""

from common import counted, get_raw, new_spark, parse, print_header, save_results

print_header("TN1 - LAZY EVALUATION VA CACHE (cells 9-11)")

spark, sc = new_spark("TN1-LazyCache")
raw = get_raw(sc)

acc_no = sc.accumulator(0)
rdd_no = raw.map(counted(parse, acc_no))
rdd_no.count(); rdd_no.count()

acc_c = sc.accumulator(0)
rdd_c = raw.map(counted(parse, acc_c)).cache()
rdd_c.count(); rdd_c.count()
rdd_c.unpersist()

results = {"so_lan_goi_khong_cache": acc_no.value,
           "so_lan_goi_co_cache": acc_c.value}
save_results("tn1_lazy_cache", results)

print(f"So lan goi parse KHONG cache (2 lan count): {acc_no.value}")
print(f"So lan goi parse CO cache    (2 lan count): {acc_c.value}")

spark.stop()
