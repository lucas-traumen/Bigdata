"""Lab 1: physical storage cost - replication vs erasure coding.

Slide 114-115: a company stores 500 TB of logs on HDFS with default
replication factor (RF) = 3. Question 2 moves the cold 80% to Erasure
Coding (6 data + 3 parity, overhead = 9/6 = 1.5x).

Homework (slide 133): same computation with 2 PB of data and RF = 2,
plus a note on the reliability trade-off.
"""


def storage_report(total_tb, rf, label):
    """Print one storage cost report; EC(6+3) overhead is fixed at 1.5x."""
    ec_overhead = 1.5
    logical_total = total_tb * rf                # Q1: everything replicated
    hot_tb = total_tb * 0.20                     # 20% hot, keep replication
    cold_tb = total_tb * 0.80                    # 80% cold, move to EC
    hot_phys = hot_tb * rf
    cold_phys = cold_tb * ec_overhead
    hybrid_total = hot_phys + cold_phys          # Q2: hybrid policy
    saving_pct = (logical_total - hybrid_total) / logical_total * 100

    print(f"--- {label} ---")
    print(f"Logical data size               : {total_tb:,.0f} TB")
    print(f"Q1: all replicated, RF={rf}          : {logical_total:,.0f} TB")
    print(f"Q2: hot 20%: {hot_tb:,.0f} TB x {rf} = {hot_phys:,.0f} TB")
    print(f"    cold 80%: {cold_tb:,.0f} TB x 1.5 = {cold_phys:,.0f} TB")
    print(f"    hybrid total                : {hybrid_total:,.0f} TB")
    print(f"    saving vs all-replication   : {saving_pct:.0f}%")
    print()
    return logical_total, hybrid_total, saving_pct


def main():
    print("Lab 1 - Storage cost: replication vs erasure coding")
    print("(all values in TB; EC(6+3): 6 data + 3 parity -> overhead 9/6 = 1.5)")
    print()

    # Main exercise (slide 114-115): 500 TB, RF = 3.
    storage_report(500, 3, "Main exercise: 500 TB, RF = 3")

    # Homework (slide 133): 2 PB and RF = 2.
    print("[Bai ve nha] same problem with 2 PB and RF = 2")
    print("(slide uses decimal units here: 2 PB = 2000 TB)")
    storage_report(2000, 2, "Homework: 2000 TB, RF = 2")

    print("[Bai ve nha] notes on the reliability trade-off:")
    print("- RF=2 tolerates only ONE DataNode failure; two concurrent failures")
    print("  can lose data. RF=3 tolerates two concurrent failures.")
    print("- EC(6+3) still tolerates 3 concurrent block losses, so in the")
    print("  hybrid plan the cold 80% is SAFER than the hot 20% (RF=2).")
    print("- Saving drops from 40% (main exercise) to 20%: with RF=2 the")
    print("  baseline already costs less, so EC adds less relative saving.")
    print("- RF=2 risks availability too: while one replica is down, only one")
    print("  copy serves reads and another failure window opens.")


if __name__ == "__main__":
    main()
