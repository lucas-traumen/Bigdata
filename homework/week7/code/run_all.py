import subprocess
import sys
from pathlib import Path

LABS = [
    "tn00_moitruong.py",
    "tn01_lazy_cache.py",
    "tn02_loc_truoc_map.py",
    "tn03_loc_truoc_shuffle.py",
    "tn04_bot_cot_gop_cuc_bo.py",
    "tn05_khong_collect_thua.py",
    "tn06_cache_dung_lai.py",
    "tn07_mappartitions.py",
    "tn08_coalesce.py",
    "tn09_tinh_dung_dan.py",
    "tn10_pipeline_tong_hop.py",
    "bt_minmax.py",
    "tonghop.py",
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
            # bien \r thanh \n de file hien thi dung
            text = out_file.read_text().replace("\r", "\n")
            out_file.write_text(text)
        else:
            print(f"!!! {lab} FAILED (exit {proc.returncode})", flush=True)

    print(f"{ok}/{len(LABS)} labs OK")
    sys.exit(0 if ok == len(LABS) else 1)

if __name__ == "__main__":
    main()
