from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Lab-Tuan4-Lab2")
         .master("local[*]")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

DATA = "data"

raw = sc.textFile(f"{DATA}/temperatures.csv")
header = raw.first()
pairs = (raw.filter(lambda l: l != header)
            .map(lambda l: l.split(","))
            .map(lambda c: (c[0], float(c[2]))))      # (station_id, temperature)

print("So ban ghi:", pairs.count())

# Xem do lech so ban ghi giua cac tram
per_station = pairs.aggregateByKey(0, lambda acc, v: acc + 1, lambda a, b: a + b)
stats = sorted(per_station.collect(), key=lambda x: -x[1])
print("Top 3 tram nhieu ban ghi nhat :", stats[:3])
print("Top 3 tram it ban ghi nhat    :", stats[-3:])

# 2a - CACH SAI: trung binh cua cac trung binh tung tram
tb_tram = (pairs.mapValues(lambda t: (t, 1))
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                .mapValues(lambda x: x[0] / x[1]))
vals = tb_tram.values().collect()
tb_sai = sum(vals) / len(vals)

# 2b - CACH DUNG: aggregateByKey giu (tong, so luong), chi chia o buoc cuoi
tong_sl = (pairs.aggregateByKey((0.0, 0),
                                lambda acc, v: (acc[0] + v, acc[1] + 1),
                                lambda a, b: (a[0] + b[0], a[1] + b[1]))
                .values()
                .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])))
tb_dung = tong_sl[0] / tong_sl[1]

print()
print("Trung binh SAI  (avg cua avg):", round(tb_sai, 2))
print("Trung binh DUNG (aggregate)  :", round(tb_dung, 2))
print("Chenh lech                   :",
      round(abs(tb_dung - tb_sai), 2), "do C")

spark.stop()
