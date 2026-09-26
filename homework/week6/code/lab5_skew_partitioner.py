import zlib
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Lab-Tuan4-Lab5")
         .master("local[*]")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

DATA = "data"

# ---- 5a: do lech tai theo endpoint ----
logs = sc.textFile(f"{DATA}/access.log")
phan_bo = (logs.map(lambda l: l.split()[3])
               .map(lambda ep: (ep, 1))
               .reduceByKey(lambda a, b: a + b)
               .sortBy(lambda x: -x[1]))
rows = phan_bo.collect()
tong = sum(n for _, n in rows)
print("Phan bo luot truy cap theo endpoint:")
for ep, n in rows:
    print("  %-16s %5d  (%.1f%%)" % (ep, n, 100.0 * n / tong))
print("Lon nhat / nho nhat =", round(rows[0][1] / rows[-1][1], 1), "lan")

# ---- 5b: custom partitioner theo domain email ----
# Dung crc32 thay built-in hash() de ket qua tai lap duoc
# (built-in hash cua str co PYTHONHASHSEED ngau nhien giua cac run)
def theo_domain(key):
    domain = key.split("@")[1] if "@" in key else "unknown"
    return zlib.crc32(domain.encode()) % 5

df = spark.read.csv(f"{DATA}/big_file.csv", header=True, inferSchema=True)
rdd = df.rdd.map(lambda r: (r["email"], r["amount"]))
kich_thuoc = (rdd.partitionBy(5, theo_domain)
                 .mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))])
                 .collect())
kich_thuoc.sort()
tong = sum(n for _, n in kich_thuoc)
print("\nKich thuoc tung partition (partitioner theo domain email):")
for i, n in kich_thuoc:
    print("  partition %d: %6d ban ghi (%.1f%%)" % (i, n, 100.0 * n / tong))

# Xu huong domain trong du lieu de giai thich phan bo
domains = (rdd.map(lambda kv: kv[0].split("@")[1] if "@" in kv[0] else "unknown")
              .countByValue())
print("\nPhan bo domain trong du lieu:")
for d, n in sorted(domains.items(), key=lambda x: -x[1]):
    print("  %-24s %6d" % (d, n))

spark.stop()
