import time
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Lab-Tuan4-Lab6")
         .master("local[*]")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

DATA = "data"

big = sc.textFile(f"{DATA}/big_file.csv").filter(lambda l: "gmail.com" in l)

# CHUA cache: RDD khong luu ket qua, moi count() tinh lai toan bo lineage
t1 = time.time(); c1 = big.count(); t2 = time.time()
c2 = big.count(); t3 = time.time()
print("Ket qua count:", c1)
print("Chua cache - lan 1: %.3fs" % (t2 - t1))
print("Chua cache - lan 2: %.3fs" % (t3 - t2))

# DA cache: lan count() dau tien nap RDD vao memory, lan sau doc tu cache
big.cache()
t4 = time.time(); c3 = big.count(); t5 = time.time()   # nap cache
t6 = time.time(); c4 = big.count(); t7 = time.time()   # doc tu cache
print("Da cache  - lan nap : %.3fs" % (t5 - t4))
print("Da cache  - lan doc : %.3fs" % (t7 - t6))

big.unpersist()
spark.stop()
