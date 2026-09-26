import json
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Lab-Tuan4-Lab3")
         .master("local[*]")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

DATA = "data"

loi_parse = sc.accumulator(0)

def parse_an_toan(dong):
    try:
        return json.loads(dong)
    except Exception:
        loi_parse.add(1)
        return None

ban_ghi = (sc.textFile(f"{DATA}/events.jsonl")
             .map(parse_an_toan)
             .filter(lambda x: x is not None))

# Demo cau hoi: doc accumulator TRUOC bat ky action nao
print("Accumulator truoc khi co action:", loi_parse.value, "(vi lazy, chua chay gi)")

n_hop_le = ban_ghi.count()

print("So ban ghi hop le:", n_hop_le)
print("So ban ghi loi   :", loi_parse.value)
print("Tong cong        :", n_hop_le + loi_parse.value)

spark.stop()
