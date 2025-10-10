# utils_single_csv.py
import os, shutil, tempfile

def _list_dir(path):
    try:
        return sorted(os.listdir(path))
    except Exception:
        return []

def write_single_csv(df, final_csv_path: str, header: bool = True):
    """
    Write Spark DF as ONE CSV file at `final_csv_path`.
    - Uses a temp dir and coalesce(1)
    - Recursively finds part-* file
    - If DF is empty, writes header-only CSV
    """
    tmp_dir = tempfile.mkdtemp(prefix="spark_csv_")  # e.g., /tmp/spark_csv_abcd1234
    try:
        # Write with Spark
        (df.coalesce(1)
           .write.mode("overwrite")
           .option("header", str(header).lower())
           .option("lineSep", "\n")
           .csv(tmp_dir))

        # Debug: show what Spark wrote
        print(f"[write_single_csv] tmp_dir = {tmp_dir}")
        print("[write_single_csv] top-level contents:", _list_dir(tmp_dir))

        # Recursively find a part-* file (handles nested committers, compression, no .csv suffix)
        part_path = None
        for root, dirs, files in os.walk(tmp_dir):
            files_sorted = sorted(files)
            # Optional: print each dir for visibility
            print(f"[write_single_csv] inspecting {root}, files={files_sorted}")
            for name in files_sorted:
                if name.startswith("part-"):
                    part_path = os.path.join(root, name)
                    break
            if part_path:
                break

        # Handle empty DataFrame (no part files at all)
        if part_path is None:
            # If DF is empty, Spark may only write _SUCCESS; create a header-only CSV
            print("[write_single_csv] No part-* found. Assuming empty DataFrame; writing header-only CSV.")
            os.makedirs(os.path.dirname(final_csv_path), exist_ok=True)
            cols = df.columns
            with open(final_csv_path, "w", encoding="utf-8", newline="") as f:
                if header and cols:
                    f.write(",".join(cols) + "\n")
            return  # done

        # Ensure destination folder exists
        os.makedirs(os.path.dirname(final_csv_path), exist_ok=True)

        # Overwrite atomically if possible
        try:
            os.replace(part_path, final_csv_path)  # replaces if exists
        except Exception:
            if os.path.exists(final_csv_path):
                os.remove(final_csv_path)
            shutil.move(part_path, final_csv_path)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
