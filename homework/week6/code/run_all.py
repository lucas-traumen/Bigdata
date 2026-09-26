import subprocess
import sys
from pathlib import Path

LABS = [
    "lab0_init.py",
    "lab1_wordcount.py",
    "lab2_average.py",
    "lab3_accumulator.py",
    "lab4_left_anti.py",
    "lab5_skew_partitioner.py",
    "lab6_cache.py",
]

def main():
    out_dir = Path(__file__).parent / "outputs"
    out_dir.mkdir(exist_ok=True)

    ok = 0
    for lab in LABS:
        name = Path(lab).stem
        out_file = out_dir / f"{name}.txt"
        print(f"=== {lab} -> outputs/{out_file.name}", flush=True)
        with open(out_file, "w") as f:
            # stdout -> file ket qua; stderr (log khung Spark/WARN) -> console
            proc = subprocess.run(
                [sys.executable, lab],
                stdout=f,
                cwd=Path(__file__).parent,
            )
        if proc.returncode == 0:
            ok += 1
            # bien \r (progress bar) thanh \n de file hien thi dung
            text = out_file.read_text().replace("\r", "\n")
            out_file.write_text(text)
        else:
            print(f"!!! {lab} FAILED (exit {proc.returncode})", flush=True)

    print(f"{ok}/{len(LABS)} labs OK")
    sys.exit(0 if ok == len(LABS) else 1)

if __name__ == "__main__":
    main()
