# ─── main.py ─────────────────────────────────────────────────────────────────
"""
Multi-Pipeline ETL Tool — Phase 2

Usage:
    python main.py              (interactive menu)
    python main.py --report     (show latest run report)
"""

import argparse
import sys
import config
from loader.db_loader import init_schema
from reporting.report import show_report


def run_pipeline(choice: str, query_choice: str = "ALL"):
    pipeline = {
        "1": "mongo", "mongo": "mongo", "mongodb": "mongo",
        "2": "pig",   "pig":   "pig",
        "3": "mapreduce", "mapreduce": "mapreduce", "mr": "mapreduce",
        "4": "hive",  "hive":  "hive",
    }.get(choice.lower())

    if not pipeline:
        print(f"[ERROR] Unknown pipeline: {choice}")
        sys.exit(1)

    print(f"\n  Pipeline : {pipeline.upper()}")
    print(f"  Query    : {query_choice}")
    print(f"  Batching : by timestamp (one batch per calendar day)\n")

    if pipeline == "mongo":
        from pipelines.mongodb_pipeline import run
        run_id = run(config.PG_CONFIG, config.MONGO_URI,
                     config.MONGO_DB, config.LOG_FILES)

    elif pipeline == "pig":
        from pipelines.pig_pipeline import run
        run_id = run(config.PG_CONFIG, config.LOG_FILES,
                     pig_home=config.PIG_HOME, pig_tmp=config.PIG_TMP)

    elif pipeline == "mapreduce":
        from pipelines.mapreduce_pipeline import run
        run_id = run(config.PG_CONFIG, config.LOG_FILES,
                     mr_tmp=config.MR_TMP_DIR)

    elif pipeline == "hive":
        from pipelines.hive_pipeline import run
        run_id = run(config.PG_CONFIG, config.LOG_FILES,
                     hive_host=config.HIVE_HOST, hive_port=config.HIVE_PORT,
                     hive_db=config.HIVE_DB)

    print("\n  Generating report …\n")
    show_report(config.PG_CONFIG, run_id=run_id)
    return run_id


def interactive_menu():
    print("\n" + "="*60)
    print("   Multi-Pipeline ETL Tool — NASA Log Analytics (Phase 2)")
    print("="*60)

    print("\n  Select execution pipeline:")
    print("    1. MongoDB")
    print("    2. Apache Pig  (local mode)")
    print("    3. MapReduce   (mrjob local)")
    print("    4. Hive        (HiveServer2)")
    print("    5. Show report for last run")
    print("    0. Exit")
    pipe_choice = input("\n  Enter choice: ").strip()

    if pipe_choice == "0":
        sys.exit(0)

    if pipe_choice == "5":
        p_filter = input(
            "  Filter by pipeline? (MongoDB/Pig/MapReduce/Hive/leave blank): "
        ).strip() or None
        show_report(config.PG_CONFIG, pipeline=p_filter)
        return

    run_pipeline(pipe_choice)


def main():
    parser = argparse.ArgumentParser(description="NASA Log ETL Tool — Phase 2")
    parser.add_argument("--pipeline",
                        choices=["mongo", "pig", "mapreduce", "hive"],
                        help="Run a pipeline directly")
    parser.add_argument("--report",    action="store_true")
    parser.add_argument("--run-id",    help="Specific run_id for report")
    parser.add_argument("--batch-id",  help="Batch to show in report, e.g. 'batch-001'. Omit to prompt interactively.")
    parser.add_argument("--init-db",   action="store_true",
                        help="Initialise PostgreSQL schema and exit")
    args = parser.parse_args()

    if args.init_db:
        init_schema(config.PG_CONFIG)
        return

    if args.report:
        show_report(config.PG_CONFIG, run_id=args.run_id,
                    pipeline=args.pipeline,
                    batch_id=getattr(args, 'batch_id', None))
    elif args.pipeline:
        run_pipeline(args.pipeline)
    else:
        while True:
            interactive_menu()


if __name__ == "__main__":
    main()
