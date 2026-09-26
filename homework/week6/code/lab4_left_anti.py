from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("Lab-Tuan4-Lab4")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

DATA = "data"

kh = spark.read.csv(f"{DATA}/customers.csv", header=True, inferSchema=True)
dh = spark.read.csv(f"{DATA}/orders.csv",    header=True, inferSchema=True)

print("Schema customers:")
kh.printSchema()
print("Schema orders:")
dh.printSchema()

key = dh.columns[1]                              # cot join trong orders
dh2 = dh.withColumnRenamed(key, key + "_dh")     # doi ten de tranh AMBIGUOUS_REFERENCE
print("\nGhi chu: ca 2 bang deu co cot", key,
      "nen join truc tiep bi loi AMBIGUOUS_REFERENCE;")
print("xu ly bang cach rename cot cua orders thanh", key + "_dh")

# 4a - left_anti
chua_mua_a = kh.join(dh2, kh[key] == dh2[key + "_dh"], "left_anti")

# 4b - left join roi loc null
chua_mua_b = (kh.join(dh2, kh[key] == dh2[key + "_dh"], "left")
                .filter(F.col(key + "_dh").isNull())
                .select(kh.columns))

print("\nleft_anti       :", chua_mua_a.count())
print("left join + null:", chua_mua_b.count())

print("\nPhysical plan cua left_anti:")
chua_mua_a.explain()

spark.stop()
