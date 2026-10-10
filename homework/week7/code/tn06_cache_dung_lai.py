"""TN6 - Nguyen tac 5: cache() khi RDD duoc dung lai (cells 24-28 notebook).

6a (buoc re): parse tren du lieu da nam trong bo nho, 3 Action.
6b (buoc dat): doc file + parse + an danh IP bang SHA-256, 5 Action
    theo NGUONG [100, 300, 500, 700, 900]. Ghi file trung gian ra work/access_x200.
"""

import hashlib
import re
import shutil

from common import bench, counted, get_base, new_spark, parse, print_header, save_results

print_header("TN6 - CACHE KHI RDD DUOC DUNG LAI (cells 24-28)")

spark, sc = new_spark("TN6-CacheDungLai")
base, n, _ = get_base(sc)

# ---------- 6a: cache buoc re ----------
print("\n--- 6a: cache buoc re (parse tren du lieu trong RAM, 3 action) ---")


def ba_action(rdd):
    return (rdd.filter(lambda r: r[0] == "ERROR").count(),
            rdd.filter(lambda r: r[4] >= 500).count(),
            rdd.map(lambda r: r[5]).sum())


def tn6a(use_cache, acc=None):
    fn = counted(parse, acc) if acc is not None else parse
    rdd = base.map(fn)
    if use_cache:
        rdd = rdd.cache()
    kq = ba_action(rdd)
    if use_cache:
        rdd.unpersist()
    return kq


a_no, a_c = sc.accumulator(0), sc.accumulator(0)
assert tn6a(False, a_no) == tn6a(True, a_c)
t_no, _ = bench(lambda: tn6a(False))
t_c, _ = bench(lambda: tn6a(True))

r6a = {
    "so_action": 3, "A_so_lan_parse": a_no.value, "B_so_lan_parse": a_c.value,
    "A_giay": t_no, "B_giay": t_c, "tang_toc": round(t_no / t_c, 1)}
save_results("tn6a_cache_buoc_re", r6a)
print(f"A so lan parse: {a_no.value} | B so lan parse: {a_c.value}")
print(f"A giay: {t_no} | B giay: {t_c} | Tang toc: {r6a['tang_toc']} lan")

# ---------- 6b: cache buoc dat ----------
print("\n--- 6b: cache buoc dat (doc file + parse + SHA-256, 5 action) ---")

SRC = "work/access_x200"
shutil.rmtree(SRC, ignore_errors=True)
base.saveAsTextFile(SRC)

IP_RE = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")


def parse_an_danh(line):
    r = parse(line)
    if not IP_RE.match(r[1]):
        return None
    ip_bam = hashlib.sha256(r[1].encode()).hexdigest()[:16]
    return (r[0], ip_bam, r[2], r[3], r[4], r[5])


NGUONG = [100, 300, 500, 700, 900]


def tn6b(use_cache, acc=None):
    fn = counted(parse_an_danh, acc) if acc is not None else parse_an_danh
    rdd = sc.textFile(SRC).map(fn)
    if use_cache:
        rdd = rdd.cache()
    kq = [rdd.filter(lambda r, t=t: r[5] > t).count() for t in NGUONG]   # vong lap 5 Action
    if use_cache:
        rdd.unpersist()
    return kq


a_no, a_c = sc.accumulator(0), sc.accumulator(0)
assert tn6b(False, a_no) == tn6b(True, a_c)
t_no, _ = bench(lambda: tn6b(False))
t_c, _ = bench(lambda: tn6b(True))

r6b = {
    "so_action": len(NGUONG), "A_so_lan_parse": a_no.value, "B_so_lan_parse": a_c.value,
    "A_giay": t_no, "B_giay": t_c, "tang_toc": round(t_no / t_c, 1)}
save_results("tn6b_cache_buoc_dat", r6b)
print(f"A so lan parse: {a_no.value} | B so lan parse: {a_c.value}")
print(f"A giay: {t_no} | B giay: {t_c} | Tang toc: {r6b['tang_toc']} lan")

base.unpersist()
spark.stop()
