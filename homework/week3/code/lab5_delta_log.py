"""Lab 5: explore the Delta Lake transaction log (_delta_log) without Spark.

Slide 124-126: normally one would run the notebooks from the
delta-lake-examples repo with PySpark installed. That repo is NOT available
on this machine (and no Spark environment exists), so this script simulates
the table folder outputs/delta_demo/ with a _delta_log/ containing three
JSON commit files (INSERT, UPDATE, DELETE) and then reads the log back.

The JSON structure follows the commit-file example on slide 125 field by
field (commitInfo / remove / add with the same keys). Every commit is one
JSON object per line (JSON Lines), like real Delta log files.
"""

import json
import os
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_DIR = os.path.join(BASE_DIR, "outputs", "delta_demo")
LOG_DIR = os.path.join(DEMO_DIR, "_delta_log")

# Fixed timestamps (ms) so the log is reproducible; slide uses 1750000001000.
TS0, TS1, TS2 = 1750000000000, 1750000001000, 1750000002000

# version -> list of JSON entries (one JSON object per line, Delta convention)
COMMITS = [
    ("000000.json", [
        {"commitInfo": {"timestamp": TS0, "operation": "INSERT",
                        "operationMetrics": {"numFiles": "1",
                                             "numRows": "2000"}}},
        {"add": {"path": "part-0001.snappy.parquet", "size": 843211,
                 "dataChange": True}},
    ]),
    # Same structure as the slide 125 example (UPDATE = remove + add pair).
    ("000001.json", [
        {"commitInfo": {"timestamp": TS1, "operation": "UPDATE",
                        "operationMetrics": {"numUpdatedRows": "120"}}},
        {"remove": {"path": "part-0001.snappy.parquet",
                    "deletionTimestamp": TS1}},
        {"add": {"path": "part-0007.snappy.parquet", "size": 843211,
                 "dataChange": True}},
    ]),
    ("000002.json", [
        {"commitInfo": {"timestamp": TS2, "operation": "DELETE",
                        "operationMetrics": {"numDeletedRows": "1"}}},
        {"remove": {"path": "part-0007.snappy.parquet",
                    "deletionTimestamp": TS2}},
    ]),
]


def main():
    # (Re)create the demo table folder deterministically.
    if os.path.isdir(DEMO_DIR):
        shutil.rmtree(DEMO_DIR)
    os.makedirs(LOG_DIR)

    for name, entries in COMMITS:
        with open(os.path.join(LOG_DIR, name), "w", encoding="ascii") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")
    print("Lab 5 - Simulated Delta table at outputs/delta_demo/_delta_log/")
    print("Files created:", ", ".join(name for name, _ in COMMITS))
    print("(simulation only: no Spark/Delta runtime; structure follows slide 125)")
    print()

    # Read the log back, version by version, and rebuild the table state.
    present = set()
    for name, _ in COMMITS:
        adds, removes, operation = 0, 0, "?"
        print(f"--- {name} ---")
        with open(os.path.join(LOG_DIR, name), encoding="ascii") as f:
            for line in f:
                rec = json.loads(line)
                if "commitInfo" in rec:
                    operation = rec["commitInfo"]["operation"]
                    print(f"    commitInfo: operation={operation}")
                elif "add" in rec:
                    adds += 1
                    present.add(rec["add"]["path"])
                    print(f"    add    : {rec['add']['path']} "
                          f"(size={rec['add']['size']})")
                elif "remove" in rec:
                    removes += 1
                    present.discard(rec["remove"]["path"])
                    print(f"    remove : {rec['remove']['path']}")
        print(f"    summary: operation={operation}, adds={adds}, "
              f"removes={removes}")
        state = ", ".join(sorted(present)) if present else "(empty table)"
        print(f"    table state after this version: {state}")
        print()

    # --- Discussion Q1 (slide 126) ---
    print("Q1: why does UPDATE create a remove+add pair instead of editing")
    print("    the old Parquet file?")
    print("    Files (and blocks) on HDFS/GFS-style storage are immutable:")
    print("    written once, then only read. Editing part-0001 in place is")
    print("    therefore impossible. Delta uses copy-on-write at FILE level:")
    print("    the commit MARKS the old file as removed and adds a new file")
    print("    (part-0007) with the rewritten content. The old file stays on")
    print("    disk until VACUUM removes it - which is what makes time travel")
    print("    and snapshot isolation possible.")

    # --- Discussion Q2 (slide 126) ---
    print()
    print("Q2: time travel to the state BEFORE the UPDATE (version 0):")
    print("    The engine replays the JSON commits from version 0 up to the")
    print("    requested version and computes: present = adds - removes.")
    print("    For version 0 it only applies 000000.json: part-0001 is still")
    print("    present and part-0007 does not exist yet. 000001.json and")
    print("    000002.json are simply NOT applied - nothing needs to be")
    print("    undone, because removed files were never physically deleted.")


if __name__ == "__main__":
    main()
