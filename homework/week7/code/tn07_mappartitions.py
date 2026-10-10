"""TN7 - Nguyen tac 6: mapPartitions cho buoc chuan bi dat tien (cells 29-31).

Mo phong buoc chuan bi bang load_lookup() - bang tra cuu 4096 IP -> vung mang.
Dung tap con 24.000 dong (small = raw.flatMap([line]*4)).
"""

import time

from common import bench, get_raw, new_spark, print_header, save_results

print_header("TN7 - MAPPARTITIONS (cells 29-31)")

spark, sc = new_spark("TN7-MapPartitions")
raw = get_raw(sc)


def load_lookup():
    # Mo phong buoc chuan bi dat tien: nap bang tra cuu IP -> vung mang
    return {f"10.0.{a}.{b}": f"zone-{a % 4}" for a in range(16) for b in range(256)}


t0 = time.perf_counter(); load_lookup(); t_setup_ms = (time.perf_counter() - t0) * 1000

small = raw.flatMap(lambda line: [line] * 4).cache()     # 24.000 dong, 8 partition
n_small = small.count()


def tn7_A(acc=None):
    def zone_of(line):
        if acc is not None: acc.add(1)
        lookup = load_lookup()                   # chuan bi: MOI DONG mot lan
        return lookup.get(line.split()[1], "unknown")
    return sorted(small.map(zone_of).countByValue().items())


def tn7_B(acc=None):
    def zone_of_partition(lines):
        if acc is not None: acc.add(1)
        lookup = load_lookup()                   # chuan bi: MOI PARTITION mot lan
        for line in lines:
            yield lookup.get(line.split()[1], "unknown")
    return sorted(small.mapPartitions(zone_of_partition).countByValue().items())


a_A, a_B = sc.accumulator(0), sc.accumulator(0)
assert tn7_A(a_A) == tn7_B(a_B)
t_A, _ = bench(tn7_A, repeat=2, warmup=False)
t_B, _ = bench(tn7_B, repeat=2, warmup=False)

results = {
    "so_dong": n_small, "chi_phi_chuan_bi_ms": round(t_setup_ms, 2),
    "A_so_lan_chuan_bi": a_A.value, "B_so_lan_chuan_bi": a_B.value,
    "A_giay": t_A, "B_giay": t_B, "tang_toc": round(t_A / t_B, 1)}
small.unpersist()
save_results("tn7_mappartitions", results)

print(f"So dong (small)              : {n_small}")
print(f"Chi phi chuan bi 1 lan (ms)  : {results['chi_phi_chuan_bi_ms']}")
print(f"A so lan chuan bi (moi dong) : {a_A.value}")
print(f"B so lan chuan bi (moi part) : {a_B.value}")
print(f"A giay: {t_A} | B giay: {t_B} | Tang toc: {results['tang_toc']} lan")

spark.stop()
