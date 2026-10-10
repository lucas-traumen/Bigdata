"""Tong hop: doc cac file JSON trong outputs/, ghi ketqua_toiuu.json,
in bang tong hop (nhu cell 41 notebook) va ve bieudo_toiuu.png (cell 43).
Nhan bieu do va nhan bang deu ASCII khong dau de tranh loi font.
"""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
OUT = HERE / "outputs"

# ---------- 1. Gop tat ca JSON ket qua tung thi nghiem ----------
RESULTS = {}
for f in sorted(OUT.glob("*.json")):
    if f.name in ("ketqua_toiuu.json",):
        continue
    RESULTS[f.stem] = json.load(open(f, encoding="utf-8"))

moi_truong = RESULTS["moi_truong"]
du_lieu = RESULTS["du_lieu"]
N = du_lieu["so_dong"]
REPEAT = 3   # dung common.py cua cac script

# ---------- 2. Bang tong hop (cell 41 notebook) ----------
t4 = RESULTS["tn4_bot_cot_gop_cuc_bo"]
rows = [
    ("NT1 Loc truoc map",         "so lan parse",   RESULTS["tn2_loc_truoc_map"]["A_so_lan_parse"],  RESULTS["tn2_loc_truoc_map"]["B_so_lan_parse"],  RESULTS["tn2_loc_truoc_map"]["A_giay"],  RESULTS["tn2_loc_truoc_map"]["B_giay"]),
    ("NT2 Loc truoc shuffle",     "byte shuffle",   RESULTS["tn3_loc_truoc_shuffle"]["A"]["shuffle_write_bytes"], RESULTS["tn3_loc_truoc_shuffle"]["B"]["shuffle_write_bytes"], RESULTS["tn3_loc_truoc_shuffle"]["A_giay"], RESULTS["tn3_loc_truoc_shuffle"]["B_giay"]),
    ("NT3 Bot cot + reduceByKey", "byte shuffle",   t4["A"]["shuffle_write_bytes"], t4["C"]["shuffle_write_bytes"], t4["A"]["giay"], t4["C"]["giay"]),
    ("NT4 Loc roi moi collect",   "dong ve Driver", RESULTS["tn5_khong_collect_thua"]["A_so_dong_ve_driver"], RESULTS["tn5_khong_collect_thua"]["B_so_dong_ve_driver"], RESULTS["tn5_khong_collect_thua"]["A_giay"], RESULTS["tn5_khong_collect_thua"]["B_giay"]),
    ("NT5a Cache (buoc re)",      "so lan parse",   RESULTS["tn6a_cache_buoc_re"]["A_so_lan_parse"], RESULTS["tn6a_cache_buoc_re"]["B_so_lan_parse"], RESULTS["tn6a_cache_buoc_re"]["A_giay"], RESULTS["tn6a_cache_buoc_re"]["B_giay"]),
    ("NT5b Cache (buoc dat)",     "so lan parse",   RESULTS["tn6b_cache_buoc_dat"]["A_so_lan_parse"], RESULTS["tn6b_cache_buoc_dat"]["B_so_lan_parse"], RESULTS["tn6b_cache_buoc_dat"]["A_giay"], RESULTS["tn6b_cache_buoc_dat"]["B_giay"]),
    ("NT6 mapPartitions",         "lan chuan bi",   RESULTS["tn7_mappartitions"]["A_so_lan_chuan_bi"], RESULTS["tn7_mappartitions"]["B_so_lan_chuan_bi"], RESULTS["tn7_mappartitions"]["A_giay"], RESULTS["tn7_mappartitions"]["B_giay"]),
    ("NT7 coalesce sau loc",      "so task",        RESULTS["tn8_coalesce"]["A"]["so_task"], RESULTS["tn8_coalesce"]["B"]["so_task"], RESULTS["tn8_coalesce"]["A_giay"], RESULTS["tn8_coalesce"]["B_giay"]),
    ("Pipeline tong hop",         "byte shuffle",   RESULTS["tn10_pipeline_tong_hop"]["A"]["shuffle_write_bytes"], RESULTS["tn10_pipeline_tong_hop"]["B"]["shuffle_write_bytes"], RESULTS["tn10_pipeline_tong_hop"]["A_giay"], RESULTS["tn10_pipeline_tong_hop"]["B_giay"]),
]
tong_hop = [{"nguyen_tac": r[0], "chi_so": r[1], "truoc": r[2], "sau": r[3],
             "truoc_s": r[4], "sau_s": r[5],
             "giam_chi_so": round(r[2] / r[3], 1), "tang_toc": round(r[4] / r[5], 1)}
            for r in rows]

with open(OUT / "ketqua_toiuu.json", "w", encoding="utf-8") as f:
    json.dump(RESULTS, f, ensure_ascii=False, indent=2)

# ---------- 3. In bang text ASCII ----------
print("=" * 104)
print(f"BANG TONG HOP - {N:,} dong, {moi_truong['so_core_spark']} core, "
      f"thoi gian = min cua {REPEAT} lan sau warmup")
print("=" * 104)
hdr = f"{'Nguyen tac':<26} {'Chi so':<16} {'Truoc':>12} {'Sau':>12} {'Giam':>7} {'X speed':>8}"
print(hdr)
print("-" * 104)
for r in tong_hop:
    print(f"{r['nguyen_tac']:<26} {r['chi_so']:<16} {r['truoc']:>12} {r['sau']:>12} "
          f"{r['giam_chi_so']:>6.1f}x {r['tang_toc']:>7.1f}x")

# ---------- 4. Bieu do (cell 43 notebook) ----------
fig, ax = plt.subplots(figsize=(10, 4.5))
x = range(len(tong_hop))
w = 0.38
ax.bar([i - w/2 for i in x], [r["truoc_s"] for r in tong_hop], w,
       label="Truoc toi uu", color="#E06666")
ax.bar([i + w/2 for i in x], [r["sau_s"] for r in tong_hop], w,
       label="Sau toi uu", color="#2E5395")
for i, r in zip(x, tong_hop):
    ax.text(i, max(r["truoc_s"], r["sau_s"]) * 1.03, f"x{r['tang_toc']}",
            ha="center", fontsize=9)
ax.set_xticks(list(x))
ax.set_xticklabels([r["nguyen_tac"] for r in tong_hop], rotation=25,
                   ha="right", fontsize=9)
ax.set_ylabel("Thoi gian (giay, min cua %d lan)" % REPEAT)
ax.set_title("Hieu qua cac nguyen tac toi uu RDD - %s dong, %d core"
             % (f"{N:,}", moi_truong["so_core_spark"]))
ax.legend(frameon=False)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
plt.tight_layout()
plt.savefig(OUT / "bieudo_toiuu.png", dpi=150)
print(f"\nDa ghi {OUT / 'ketqua_toiuu.json'} va {OUT / 'bieudo_toiuu.png'}")
