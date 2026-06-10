"""
==============================================================
pipeline_parallel.py  -  Day 10 Lab (Advanced)
==============================================================
Student ID: AI20K-1008
Name: Do VA

Nang cap tu solution.py: thay vi xu ly 1 file JSON, pipeline nay
xu ly SONG SONG nhieu file CSV trong mot thu muc (data_lake/).

Diem moi so voi ban goc:
   1. Multi-file:   quet tat ca *.csv trong 1 folder.
   2. Robust ETL:   chiu duoc schema khac nhau, gia ban
                    ("$1,200", "ten dollars", "N/A", so am, null,
                    khoa hoc 1.2e3), category hoa/thuong/whitespace,
                    va file THIEU cot 'category'.
   3. Parallelism:  so sanh 3 che do -> Sequential / ThreadPool /
                    ProcessPool (concurrent.futures) va do thoi gian.
   4. Global dedup: loai trung ID XUYEN nhieu file.
   5. Outlier rule: loai gia bat thuong bang IQR (Tukey's fences)
                    tren TOAN BO phan phoi sau khi gop (vd 999999).
   6. Observability: log per-file (read/kept/dropped + ly do),
                    them cot 'source_file' + 'processed_at'.

Chay:
   python generate_data_lake.py     # tao data_lake/
   python pipeline_parallel.py      # chay pipeline song song
==============================================================
"""

import os
import glob
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import pandas as pd

# --- CONFIGURATION ---
INPUT_DIR = "data_lake"
OUTPUT_FILE = "processed_lake.csv"
MAX_WORKERS = os.cpu_count() or 4

# Cot chuan cua output (thu tu co dinh du input co schema khac nhau)
OUTPUT_COLUMNS = [
    "id", "product", "price", "category",
    "discounted_price", "processed_at", "source_file",
]


# ============================================================
# WORKER: ETL cho MOT file (chay duoc trong process/thread rieng)
# Phai la ham top-level de ProcessPoolExecutor pickle duoc.
# ============================================================
def process_one_file(file_path):
    """Extract -> Validate -> Transform cho 1 file CSV.

    Returns:
        dict gom:
          - 'df'    : DataFrame da lam sach (hoac None neu loi)
          - 'stats' : thong ke observability cua file nay
    """
    name = os.path.basename(file_path)
    stats = {
        "file": name, "rows_read": 0, "kept": 0, "dropped": 0,
        "reason_price": 0, "reason_category": 0,
        "missing_category_col": False, "error": None,
    }

    try:
        # --- EXTRACT: doc tat ca duoi dang string de tu kiem soat parse
        df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
        stats["rows_read"] = len(df)

        # --- ROBUST: chuan hoa price (bo "$", ",", whitespace -> so)
        price_raw = df.get("price", pd.Series([""] * len(df)))
        price_clean = (
            price_raw.astype(str).str.strip().str.replace(r"[$,]", "", regex=True)
        )
        df["_price_num"] = pd.to_numeric(price_clean, errors="coerce")

        # --- ROBUST: chuan hoa category (xu ly file THIEU cot category)
        if "category" not in df.columns:
            stats["missing_category_col"] = True
            df["category"] = ""
        cat = df["category"].astype(str).str.strip()
        cat_empty = (cat == "") | cat.str.lower().isin(["nan", "none"])

        # --- VALIDATE: price > 0 (khong NaN) va category khong rong
        price_bad = df["_price_num"].isna() | (df["_price_num"] <= 0)
        valid_mask = (~price_bad) & (~cat_empty)

        stats["kept"] = int(valid_mask.sum())
        stats["dropped"] = int((~valid_mask).sum())
        stats["reason_price"] = int(price_bad.sum())
        # chi dem category-only de khong trung voi reason_price
        stats["reason_category"] = int((cat_empty & ~price_bad).sum())

        # --- TRANSFORM: tren tap hop le
        out = df[valid_mask].copy()
        out["price"] = out["_price_num"]
        out["product"] = out.get("product", "").astype(str).str.strip()
        out["category"] = out["category"].astype(str).str.strip().str.title()
        out["discounted_price"] = out["price"] * 0.9
        out["processed_at"] = datetime.datetime.now().isoformat()
        out["source_file"] = name

        # bao dam du cot output (id co the thieu o file la)
        for col in OUTPUT_COLUMNS:
            if col not in out.columns:
                out[col] = ""
        out = out[OUTPUT_COLUMNS]

        return {"df": out, "stats": stats}

    except Exception as e:  # observability: khong cho 1 file lam sap ca pipeline
        stats["error"] = str(e)
        return {"df": None, "stats": stats}


# ============================================================
# RUNNERS: 3 che do thuc thi de SO SANH thoi gian
# ============================================================
def run_sequential(files):
    return [process_one_file(f) for f in files]


def run_threadpool(files):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        return list(ex.map(process_one_file, files))


def run_processpool(files):
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        return list(ex.map(process_one_file, files))


def timed(label, fn, files):
    """Chay 1 runner, do wall-clock, in ket qua."""
    t0 = time.perf_counter()
    results = fn(files)
    elapsed = time.perf_counter() - t0
    total_kept = sum(r["stats"]["kept"] for r in results)
    print(f"  {label:<14} | {elapsed:7.3f}s | {total_kept:,} records kept")
    return results, elapsed


# ============================================================
# MERGE + GLOBAL DEDUP + LOAD
# ============================================================
def consolidate(results):
    frames = [r["df"] for r in results if r["df"] is not None and not r["df"].empty]
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), 0

    merged = pd.concat(frames, ignore_index=True)
    before = len(merged)
    # dedup ID xuyen file: giu ban ghi dau tien
    merged = merged.drop_duplicates(subset="id", keep="first").reset_index(drop=True)
    dupes_removed = before - len(merged)
    return merged, dupes_removed


def remove_outliers_iqr(df, column="price", k=1.5):
    """Loai outlier bang phuong phap IQR (Tukey's fences).

    Vi sao chay GLOBAL (sau khi gop) chu khong per-file?
       - Outlier la khai niem THONG KE, can xet tren ca quan the.
         Mot gia tri "binh thuong" trong 1 file nho co the la outlier
         khi nhin tren toan bo data lake (va nguoc lai).

    Cong thuc:
       Q1, Q3 = phan vi 25% va 75% cua cot gia
       IQR    = Q3 - Q1
       fence  = [Q1 - k*IQR, Q3 + k*IQR]   (k=1.5 theo chuan Tukey)
       Gia tri ngoai khoang [fence] -> outlier -> loai.

    Returns:
        (df_clean, stats_dict)
    """
    if df.empty:
        return df, {"removed": 0, "lower": None, "upper": None}

    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    lower = q1 - k * iqr
    upper = q3 + k * iqr

    mask = (df[column] >= lower) & (df[column] <= upper)
    removed = int((~mask).sum())
    stats = {"removed": removed, "lower": round(lower, 2), "upper": round(upper, 2)}
    return df[mask].reset_index(drop=True), stats


def print_observability(results, dupes_removed, outlier_stats, final_count):
    print("\n" + "=" * 64)
    print("OBSERVABILITY REPORT (per-file)")
    print("=" * 64)
    header = f"{'file':<18}{'read':>8}{'kept':>8}{'dropped':>9}{'price':>7}{'cat':>6}"
    print(header)
    print("-" * 64)
    tot_read = tot_kept = tot_drop = 0
    for r in results:
        s = r["stats"]
        flag = "  [!] no category column" if s["missing_category_col"] else ""
        if s["error"]:
            flag = f"  [ERROR] {s['error']}"
        print(f"{s['file']:<18}{s['rows_read']:>8}{s['kept']:>8}"
              f"{s['dropped']:>9}{s['reason_price']:>7}{s['reason_category']:>6}{flag}")
        tot_read += s["rows_read"]
        tot_kept += s["kept"]
        tot_drop += s["dropped"]
    print("-" * 64)
    print(f"{'TOTAL':<18}{tot_read:>8}{tot_kept:>8}{tot_drop:>9}")
    print(f"\nGlobal duplicate IDs removed (cross-file): {dupes_removed:,}")
    print(f"IQR outliers removed (price outside "
          f"[{outlier_stats['lower']}, {outlier_stats['upper']}]): "
          f"{outlier_stats['removed']:,}")
    print(f"Final clean records written:               {final_count:,}")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 64)
    print("PARALLEL ETL PIPELINE (multi-CSV)")
    print("=" * 64)

    files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.csv")))
    if not files:
        print(f"No CSV files found in ./{INPUT_DIR}/. "
              f"Run: python generate_data_lake.py first.")
        return

    print(f"Found {len(files)} CSV files in ./{INPUT_DIR}/  "
          f"(workers={MAX_WORKERS})\n")

    # --- SO SANH 3 CHE DO ---
    print("Benchmark (Mode | wall-clock | output):")
    run_sequential_results, t_seq = timed("Sequential", run_sequential, files)
    _, t_thread = timed("ThreadPool", run_threadpool, files)
    results, t_proc = timed("ProcessPool", run_processpool, files)

    print("\nSpeedup vs Sequential:")
    print(f"  ThreadPool : {t_seq / t_thread:5.2f}x")
    print(f"  ProcessPool: {t_seq / t_proc:5.2f}x")

    # --- MERGE + DEDUP + OUTLIER + LOAD (1 lan, dung ket qua ProcessPool) ---
    merged, dupes_removed = consolidate(results)
    merged, outlier_stats = remove_outliers_iqr(merged, column="price", k=1.5)
    merged.to_csv(OUTPUT_FILE, index=False)

    print_observability(results, dupes_removed, outlier_stats, len(merged))
    print(f"\nSaved consolidated clean data -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
