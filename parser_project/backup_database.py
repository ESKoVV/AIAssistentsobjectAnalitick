from __future__ import annotations

import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from config import load_config, validate_db_config


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a pg_dump backup before destructive schema changes.")
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output file path. Defaults to backups/hack_db-<timestamp>.dump",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = load_config()
    validate_db_config(config)

    if args.output:
        output_path = Path(args.output)
    else:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = Path("backups") / f"hack_db-{timestamp}.dump"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "pg_dump",
            "--format=custom",
            "--file",
            str(output_path),
            config.database_url,
        ],
        check=True,
    )
    print(f"✅ Backup created: {output_path}")


if __name__ == "__main__":
    main()
