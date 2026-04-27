# ─── main.py ─────────────────────────────────────────────────────────────────
"""
Multi-Pipeline ETL Tool — Entry Point

Usage:
    python main.py                  # interactive menu
    python main.py --pipeline mongo # run MongoDB pipeline directly
    python main.py --pipeline pig   # run Pig pipeline directly
    python main.py --report         # show report for last run
    python main.py --report --pipeline mongo  # report for last MongoDB run
"""

import argparse
import sys

import config as cfg
from reporting.report import show_report


CONFIG = {
    "LOG_FILES":  cfg.LOG_FILES,
    "BATCH_SIZE": cfg.BATCH_SIZE,
    "PG_CONFIG":  cfg.PG_CONFIG,
    "MONGO_URI":  cfg.MONGO_URI,
    "MONGO_DB":   cfg.MONGO_DB,
    "MONGO_COL":  cfg.MONGO_COL,
    "MR_TMP_DIR": cfg.MR_TMP_DIR,
}


def run_pipeline(choice: str):
    if choice in ("1", "mongo", "mongodb"):
        from pipelines.mongodb_pipeline import run
        run_id = run(CONFIG)
    elif choice in ("2", "pig"):
        from pipelines.pig_pipeline import run
        run_id = run(CONFIG)
    else:
        print(f"Unknown pipeline choice: {choice}")
        sys.exit(1)

    print(f"\nPipeline complete. Generating report for run {run_id}...\n")
    show_report(CONFIG["PG_CONFIG"], run_id=run_id)


def interactive_menu():
    print("\n" + "="*50)
    print("  Multi-Pipeline ETL Tool — NASA Log Analytics")
    print("="*50)
    print("  Select execution pipeline:")
    print("    1. MongoDB")
    print("    2. Apache Pig (Mapreduce on Hadoop)")
    print("    3. Show report for last run")
    print("    0. Exit")
    print("="*50)
    choice = input("  Enter choice: ").strip()

    if choice == "0":
        sys.exit(0)
    elif choice == "3":
        pipeline_filter = input("  Filter by pipeline? (mongo/mr/leave blank): ").strip() or None
        show_report(CONFIG["PG_CONFIG"], pipeline=pipeline_filter)
    else:
        run_pipeline(choice)


def main():
    parser = argparse.ArgumentParser(description="NASA Log ETL Tool")
    parser.add_argument("--pipeline", choices=["mongo", "pig"], help="Pipeline to run")
    parser.add_argument("--report",   action="store_true", help="Show report only")
    parser.add_argument("--run-id",   help="Specific run ID for report")
    args = parser.parse_args()

    if args.report:
        show_report(CONFIG["PG_CONFIG"], run_id=args.run_id,
                    pipeline=args.pipeline)
    elif args.pipeline:
        run_pipeline(args.pipeline)
    else:
        while True:
            interactive_menu()


if __name__ == "__main__":
    main()
