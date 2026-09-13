"""Lab 3: simulate HDFS rack-aware replica placement in Python (slide 118-120).

Policy: replica 1 on the writing client node, replica 2 on a node in a
DIFFERENT rack, replica 3 on ANOTHER node of that second rack.
The code below matches slide 119 exactly; the two discussion questions
(slide 120) are demonstrated at the end.
"""

import random

nodes = {  # node: rack
    "n1": "r1", "n2": "r1", "n3": "r1",
    "n4": "r2", "n5": "r2", "n6": "r2",
    "n7": "r3", "n8": "r3",
}


def place_replicas(client_node, nodes, seed=1):
    random.seed(seed)
    r1 = client_node
    rack1 = nodes[r1]
    # NOTE: despite its name, "other_racks" holds NODES living in other racks
    other_racks = [n for n, r in nodes.items() if r != rack1]
    r2 = random.choice(other_racks)
    rack2 = nodes[r2]
    same_rack2 = [n for n, r in nodes.items() if r == rack2 and n != r2]
    r3 = random.choice(same_rack2)
    return [(r1, rack1), (r2, rack2), (r3, rack2)]


def place_replicas_rf5(client_node, nodes, seed=1):
    """Homework-style extension for discussion Q2 (slide 120).

    Places 5 replicas with at most 2 per rack: replica 1 on the client node,
    then 2 nodes from each of two other racks (>= 3 racks used in total).
    """
    random.seed(seed)
    racks = {}
    for n, r in nodes.items():
        racks.setdefault(r, []).append(n)
    rack1 = nodes[client_node]
    placement = [(client_node, rack1)]  # replica 1: writing client node
    other_racks = [r for r in racks if r != rack1]
    if len(other_racks) < 2:
        raise ValueError("RF=5 with max 2 replicas/rack needs at least 3 racks")
    for rack in other_racks[:2]:
        pool = [n for n in racks[rack] if n != client_node]
        if len(pool) < 2:
            raise ValueError(f"rack {rack} has fewer than 2 spare nodes")
        picked = random.sample(pool, 2)
        placement.extend((n, rack) for n in picked)
    return placement


def main():
    print("Lab 3 - Rack-aware placement simulation (8 nodes / 3 racks)")
    print("Cluster layout:")
    for node, rack in nodes.items():
        print(f"  {node}: {rack}")
    print()

    print("place_replicas('n1', nodes, seed=1):")
    result = place_replicas("n1", nodes, seed=1)
    for node, rack in result:
        print(f"  replica on {node} (rack {rack})")
    print("(slide 119 shows the example ('n1','r1'), ('n5','r2'), ('n4','r2');")
    print(" the concrete nodes picked depend on the Python random version,")
    print(" seed=1 is recorded so the run is reproducible)")
    print()

    # Verify the placement policy programmatically.
    ok1 = result[0][0] == "n1"
    ok2 = result[1][1] != result[0][1]
    ok3 = result[2][1] == result[1][1] and result[2][0] != result[1][0]
    print("Policy check:")
    print(f"  replica 1 on the writing client node        : "
          f"{'PASS' if ok1 else 'FAIL'}")
    print(f"  replica 2 on a DIFFERENT rack than the client: "
          f"{'PASS' if ok2 else 'FAIL'} ({result[1][0]} in {result[1][1]})")
    print(f"  replica 3 on ANOTHER node of rack {result[1][1]}         : "
          f"{'PASS' if ok3 else 'FAIL'} ({result[2][0]} in {result[2][1]})")
    print()

    # --- Discussion Q1 (slide 120): cluster with only ONE rack ---
    print("Q1: what if the cluster has only ONE rack?")
    single_rack = {"n1": "r1", "n2": "r1", "n3": "r1"}
    try:
        place_replicas("n1", single_rack, seed=1)
        print("  no error (unexpected)")
    except IndexError as exc:
        print(f"  IndexError: {exc}")
        print("  -> raised at random.choice(other_racks): that list is EMPTY")
        print("     because no node lives outside rack r1.")
    print("  Why this is dangerous for fault tolerance: every replica would")
    print("  sit on the same rack; one rack-level failure (top-of-rack switch,")
    print("  power circuit) destroys ALL copies at once - replication by rack")
    print("  is the whole point of the policy.")
    print()

    # --- Discussion Q2 (slide 120): RF = 5, max 2 replicas per rack ---
    print("Q2: RF = 5 with at most 2 replicas per rack")
    print("  Strategy: 1 replica on the client node, then 2 nodes from EACH")
    print("  of two other racks -> 5 replicas over 3 racks, max 2 per rack.")
    res5 = place_replicas_rf5("n1", nodes, seed=1)
    for node, rack in res5:
        print(f"  replica on {node} (rack {rack})")
    per_rack = {}
    for node, rack in res5:
        per_rack[rack] = per_rack.get(rack, 0) + 1
    ok_max2 = all(c <= 2 for c in per_rack.values())
    print(f"  replicas per rack: {per_rack} (rule 'at most 2 per rack': "
          f"{'PASS' if ok_max2 else 'FAIL'})")
    print("  Note: with only 2 racks this is impossible without breaking the")
    print("  rule - 5 replicas over 2 racks force 3 replicas on some rack.")


if __name__ == "__main__":
    main()
