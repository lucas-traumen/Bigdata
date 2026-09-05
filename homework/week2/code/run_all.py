"""Run all 6 labs in order via subprocess with the same Python executable.

Saves each lab's stdout to code/outputs/labN.txt and prints a summary.
Exits non-zero if any lab fails.
"""

import os
import subprocess
import sys

LABS = [
    ("lab1_tfidf.py", "outputs/lab1.txt"),
    ("lab2_bonferroni.py", "outputs/lab2.txt"),
    ("lab3_kmeans.py", "outputs/lab3.txt"),
    ("lab4_association_rules.py", "outputs/lab4.txt"),
    ("lab5_decision_tree.py", "outputs/lab5.txt"),
    ("lab6_linear_regression.py", "outputs/lab6.txt"),
]

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    outputs_dir = os.path.join(base_dir, "outputs")
    os.makedirs(outputs_dir, exist_ok=True)

    results = []
    for idx, (script, out_rel) in enumerate(LABS, start=1):
        script_path = os.path.join(base_dir, script)
        out_path = os.path.join(base_dir, out_rel)
        print(f"=== Lab {idx}: {script} ===")
        try:
            proc = subprocess.run(
                [sys.executable, script_path],
                capture_output=True, text=True, timeout=300,
            )
        except subprocess.TimeoutExpired:
            print(f"Lab {idx}: FAILED (timeout)")
            results.append((idx, False, "timeout"))
            continue

        sys.stdout.write(proc.stdout)
        if proc.returncode != 0:
            sys.stderr.write(proc.stderr)
            print(f"Lab {idx}: FAILED (exit code {proc.returncode})")
            results.append((idx, False, f"exit code {proc.returncode}"))
            continue

        with open(out_path, "w") as f:
            f.write(proc.stdout)
        print(f"Lab {idx}: OK (output saved)")
        results.append((idx, True, None))

    print("\n=== Summary ===")
    for idx, ok, err in results:
        if ok:
            print(f"Lab {idx}: OK (output saved)")
        else:
            print(f"Lab {idx}: FAILED ({err})")

    if any(not ok for _, ok, _ in results):
        sys.exit(1)

if __name__ == "__main__":
    main()
