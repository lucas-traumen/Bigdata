from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("Lab-Tuan4-Lab0")
         .master("local[*]")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("Spark version :", spark.version)
print("Java version  :", sc._jvm.System.getProperty("java.version"))
print("Master        :", sc.master)
print("So core       :", sc.defaultParallelism)
print("Spark UI      :", sc.uiWebUrl)
print()
print("Cluster manager: LOCAL mode - khong co cluster manager ngoai (YARN/K8s)")
print("local[*] -> dung 1 Executor duy noi tren JVM driver,")
print("   voi", sc.defaultParallelism, "thread = so core CPU kha dung cua may")

spark.stop()
