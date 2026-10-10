"""common.py - dung chung cho cac script Tuan 7 (chuyen tu buoi1_toiuu.ipynb).

Giu nguyen thuat toan va ten ham cua notebook: parse, counted, bench,
stage_metrics, run_in_group, nhan_ban. Moi script tu dung SparkSession rieng
 nen so job/stage giua cac thi nghiem khong nhiem sang nhau.
"""

import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

# Dam bao worker Python cung mot trinh thong dich voi driver (cell 2 notebook)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession

SCALE = 200     # nhan ban access.log 200 lan -> 1.2 trieu dong
REPEAT = 3      # so lan do (lay min) sau 1 lan chay nong

HERE = Path(__file__).resolve().parent


def print_header(title):
    """In dong ke + tieu de ASCII de doc output de (khong dung dau tieng Viet)."""
    print("=" * 70)
    print(title)
    print("=" * 70)


def new_spark(app_name):
    """Khoi tao SparkSession giong cell 2 cua notebook."""
    spark = (SparkSession.builder
             .appName(f"Tuan7-{app_name}")
             .master("local[*]")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark, sc


def parse(line):
    """Tach mot dong log thanh tuple 6 truong (cell 8 notebook)."""
    c = line.split()
    return (c[0], c[1], c[2], c[3], int(c[4]), int(c[5][:-2]))


def counted(fn, acc):
    """Boc ham: moi lan duoc goi thi cong 1 vao Accumulator."""
    def wrapper(x):
        acc.add(1)
        return fn(x)
    return wrapper


def bench(fn, repeat=REPEAT, warmup=True):
    """Chay nong 1 lan, roi do REPEAT lan, tra ve thoi gian nho nhat."""
    if warmup:
        fn()
    best, out = float("inf"), None
    for _ in range(repeat):
        t0 = time.perf_counter()
        out = fn()
        best = min(best, time.perf_counter() - t0)
    return round(best, 3), out


def stage_metrics(group, sc):
    """Doc REST API Spark UI: so stage/task, byte/record shuffle cua job group."""
    time.sleep(1.5)   # listener cua Spark UI cap nhat bat dong bo
    api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    jobs = json.load(urlopen(api + "/jobs"))
    stage_ids = {s for j in jobs if j.get("jobGroup") == group for s in j["stageIds"]}
    m = {"so_stage": 0, "so_task": 0, "shuffle_write_bytes": 0,
         "shuffle_write_records": 0, "shuffle_read_bytes": 0}
    for sid in stage_ids:
        for att in json.load(urlopen(f"{api}/stages/{sid}")):
            if att["status"] != "COMPLETE":
                continue
            m["so_stage"] += 1
            m["so_task"] += att["numCompleteTasks"]
            m["shuffle_write_bytes"] += att["shuffleWriteBytes"]
            m["shuffle_write_records"] += att["shuffleWriteRecords"]
            m["shuffle_read_bytes"] += att["shuffleReadBytes"]
    return m


def run_in_group(group, fn, sc):
    """Gan job group truoc khi chay Action de stage_metrics do dung job."""
    sc.setJobGroup(group, group)
    try:
        return fn()
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)


def nhan_ban(line):
    """Nhan ban mot dong log x200, bien doi nhe tung ban sao (cell 6 notebook)."""
    level, ip, method, endpoint, status, latency = line.split()
    a, b, c, d = ip.split(".")
    ms = int(latency[:-2])
    for k in range(SCALE):
        yield (f"{level} {a}.{b}.{c}.{(int(d) + 37 * k) % 256} {method} {endpoint} "
               f"{status} {max(1, ms + (k * 13) % 41 - 20)}ms")


def get_raw(sc):
    """Doc access.log: 6000 dong, 8 partition."""
    return sc.textFile("data/access.log", 8)


def get_base(sc, raw=None):
    """Dung base = flatMap(nhan_ban) x200, cache + count (cell 6 notebook).

    Tra ve (base, N, raw). Cache de loai thoi gian doc file khoi moi phep do.
    """
    if raw is None:
        raw = get_raw(sc)
    base = raw.flatMap(nhan_ban).cache()
    n = base.count()
    return base, n, raw


def save_results(key, value):
    """Ghi outputs/<key>.json (tao thu muc neu chua co)."""
    out_dir = HERE / "outputs"
    out_dir.mkdir(exist_ok=True)
    with open(out_dir / f"{key}.json", "w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)


def cpu_name():
    """Ten CPU: Linux doc /proc/cpuinfo, macOS dung sysctl (cell 4 notebook)."""
    try:
        # Linux: dong "model name" trong /proc/cpuinfo
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    return line.split(":", 1)[1].strip()
    except Exception:
        pass
    try:
        return subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"]).decode().strip()
    except Exception:
        return platform.processor() or "unknown"


def ram_gb():
    """Dung luong RAM (GB) nhu cell 4 cua notebook."""
    try:
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (ValueError, OSError, AttributeError):
        try:   # macOS khong ho tro SC_PHYS_PAGES
            return round(int(subprocess.check_output(["sysctl", "-n", "hw.memsize"])) / 2**30, 1)
        except Exception:
            return None
