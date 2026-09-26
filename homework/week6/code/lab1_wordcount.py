from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Lab-Tuan4-Lab1")
         .master("local[*]")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

DATA = "data"

# Ghi chu trung thuc: data khong co file document.txt (300 dong van ban)
# nhu de bai nen lab dung access.log lam ngu lieu van ban thay the.
lines = sc.textFile(f"{DATA}/access.log")
print("Nguon lieu : access.log (thay the document.txt khong co trong data/)")
print("So dong    :", lines.count())

# 1a - WordCount bang reduceByKey (co combine tai partition truoc shuffle)
wc_reduce = (lines.flatMap(lambda l: l.split())
                  .map(lambda w: w.lower())
                  .map(lambda w: (w, 1))
                  .reduceByKey(lambda a, b: a + b))

# 1b - WordCount bang groupByKey + mapValues (khong combine, shuffle toan bo)
wc_group = (lines.flatMap(lambda l: l.split())
                 .map(lambda w: w.lower())
                 .map(lambda w: (w, 1))
                 .groupByKey()
                 .mapValues(lambda vals: sum(vals)))

top5 = sorted(wc_reduce.collect(), key=lambda x: -x[1])[:5]
print("\nTop 5 tu (reduceByKey):")
for w, n in top5:
    print("  %-16s %6d" % (w, n))

same = sorted(wc_reduce.collect()) == sorted(wc_group.collect())
print("\nHai cach cho cung ket qua:", same)

# Dem so tu rieng biet de thay muc gia tri cua combine
n_distinct = wc_reduce.count()
print("So tu rieng biet :", n_distinct)
print("So cap (tu, 1) truoc reduce/group =", lines.flatMap(lambda l: l.split()).count())
print("  -> reduceByKey combine gom", n_distinct, "key ngay tai partition,")
print("     groupByKey phai shuffle toan bo cap (tu, 1) qua mang.")

spark.stop()
