"""Lab 6: choose the right table format for each scenario (slide 127-129).

For each situation pick Delta Lake / Iceberg / Hudi and give a short reason
linked to the characteristics studied in the theory part.
"""

SCENARIOS = [
    ("A",
     "Position updates from millions of IoT devices every few seconds; "
     "continuous upserts required",
     "Apache Hudi",
     "core strength is upsert / near real-time ingest (record-level index, "
     "copy-on-write / merge-on-read) built exactly for this workload"),
    ("B",
     "One shared table read by Spark, Trino AND Flink; multi-engine required",
     "Apache Iceberg",
     "designed as an engine-independent table spec; hidden partitioning keeps "
     "plans consistent across engines without partition hints"),
    ("C",
     "Whole infrastructure already on Databricks; deep integration and simple "
     "operation preferred",
     "Delta Lake",
     "deepest integration with the Databricks/Spark ecosystem and the "
     "simplest operation in that context"),
]


def main():
    print("Lab 6 - Table format choice per scenario (slide 127-129)")
    print()
    for code, workload, choice, why in SCENARIOS:
        print(f"[Scenario {code}]")
        print(f"  Workload: {workload}")
        print(f"  Choice  : {choice}")
        print(f"  Reason  : {why}")
        print()
    print("Rules of thumb: Hudi = heavy upserts; Iceberg = multi-engine, big")
    print("analytic tables; Delta = Spark/Databricks-centric stacks.")


if __name__ == "__main__":
    main()
