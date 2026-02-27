import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

def setup_logging(log_file: str) -> logging.Logger:
    logger = logging.getLogger("mlops_job")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")


    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def write_metrics(output_path: str, payload: dict) -> None:
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)


def write_error(output_path: str, version: str, message: str) -> None:
    payload = {
        "version": version,
        "status": "error",
        "error_message": message,
    }
    write_metrics(output_path, payload)
    print(json.dumps(payload, indent=2))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MLOps rolling-mean signal job")
    parser.add_argument("--input",    required=True, help="Path to input CSV file")
    parser.add_argument("--config",   required=True, help="Path to YAML config file")
    parser.add_argument("--output",   required=True, help="Path to output metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path to log file")
    args = parser.parse_args()

    logger = setup_logging(args.log_file)
    t_start = time.time()
    logger.info("=== Job started ===")

    version = "v1" 

    config_path = Path(args.config)
    if not config_path.exists():
        msg = f"Config file not found: {args.config}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        msg = f"Failed to parse config YAML: {e}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    required_keys = {"seed", "window", "version"}
    missing = required_keys - set(config or {})
    if missing:
        msg = f"Config missing required fields: {sorted(missing)}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    seed    = config["seed"]
    window  = config["window"]
    version = config["version"]

    if not isinstance(seed, int):
        msg = f"'seed' must be an integer, got {type(seed).__name__}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    if not isinstance(window, int) or window < 1:
        msg = f"'window' must be a positive integer, got {window!r}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    logger.info(f"Config loaded + validated: version={version}, seed={seed}, window={window}")
    np.random.seed(seed)

    input_path = Path(args.input)
    if not input_path.exists():
        msg = f"Input file not found: {args.input}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        msg = f"Failed to read CSV: {e}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    if df.empty:
        msg = "Input CSV is empty"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    if "close" not in df.columns:
        msg = f"Required column 'close' not found. Columns present: {list(df.columns)}"
        logger.error(msg)
        write_error(args.output, version, msg)
        sys.exit(1)

    logger.info(f"Rows loaded: {len(df)}")

    # 3. Rolling mean
    df["rolling_mean"] = df["close"].rolling(window=window).mean()
    logger.info(f"Rolling mean computed with window={window}")

    # 4. Signal generation
    valid_mask = df["rolling_mean"].notna()
    df.loc[valid_mask, "signal"] = (
        df.loc[valid_mask, "close"] > df.loc[valid_mask, "rolling_mean"]
    ).astype(int)

    rows_processed = len(df)
    signal_rate = float(df.loc[valid_mask, "signal"].mean())
    logger.info(f"Signal generated: rows_processed={rows_processed}, signal_rate={signal_rate:.4f}")

    # 5. Metrics + timing
    latency_ms = int((time.time() - t_start) * 1000)

    metrics = {
        "version": version,
        "rows_processed": rows_processed,
        "metric": "signal_rate",
        "value": round(signal_rate, 4),
        "latency_ms": latency_ms,
        "seed": seed,
        "status": "success",
    }

    write_metrics(args.output, metrics)
    logger.info(f"Metrics summary: {metrics}")
    logger.info(f"Job completed successfully!  latency={latency_ms}ms, status=success")

    print(json.dumps(metrics, indent=2))
    sys.exit(0)


if __name__ == "__main__":
    main()