import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"


def run_cmd(cmd: list[str], step_name: str):
    """Run a subprocess command and stream output."""
    print(f"\n===== START STEP: {step_name} =====")
    print("Command:", " ".join(cmd))
    start_time = datetime.utcnow()

    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            check=True,
        )
        end_time = datetime.utcnow()
        print(f"===== STEP SUCCESS: {step_name} (took {end_time - start_time}) =====\n")
        return result
    except subprocess.CalledProcessError as e:
        end_time = datetime.utcnow()
        print(f"***** STEP FAILED: {step_name} (took {end_time - start_time}) *****")
        print("Return code:", e.returncode)
        # Optional: re-raise to stop pipeline
        raise


def main():
    # Make sure we are at project root
    os.chdir(PROJECT_ROOT)
    print("Project root:", PROJECT_ROOT)

    # STEP 1: Generate raw transactions
    # You can tweak n_rows if you like
    n_rows = 1000
    print(f"Preparing to generate {n_rows} fake transactions...")

    gen_cmd = [
        sys.executable,  # uses current venv's python
        str(SRC_DIR / "transaction_generator.py"),
        "--n_rows",
        str(n_rows),
    ]
    run_cmd(gen_cmd, step_name="Generate raw transactions")

    # STEP 2: Bronze ingest
    bronze_cmd = [
        sys.executable,
        str(SRC_DIR / "bronze_ingest_pyspark.py"),
    ]
    run_cmd(bronze_cmd, step_name="Bronze ingest")

    # STEP 3: Silver transform
    silver_cmd = [
        sys.executable,
        str(SRC_DIR / "silver_transform_pyspark.py"),
    ]
    run_cmd(silver_cmd, step_name="Silver transform")

    # STEP 4: Gold features
    gold_cmd = [
        sys.executable,
        str(SRC_DIR / "gold_features_pyspark.py"),
    ]
    run_cmd(gold_cmd, step_name="Gold features")

    print("\n🎉 Pipeline finished successfully! Bronze → Silver → Gold are up to date.\n")


if __name__ == "__main__":
    main()
