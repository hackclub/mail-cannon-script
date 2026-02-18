#!/usr/bin/env python3
"""
mail-cannon: Batch warehouse order creator for Theseus.

Reads a CSV of recipients and SKU quantities, creates a warehouse order
on Theseus for each row, and writes detailed logs to a timestamped file.

Usage:
    python mail_cannon.py orders.csv
    python mail_cannon.py orders.csv --config config.json --dry-run
"""

import argparse
import csv
import json
import logging
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ADDRESS_COLUMNS = [
    "first_name",
    "last_name",
    "email",
    "line_1",
    "line_2",
    "city",
    "state",
    "postal_code",
    "country",
]

REQUIRED_ADDRESS_COLUMNS = [
    "first_name",
    "email",
    "line_1",
    "city",
    "state",
    "postal_code",
    "country",
]

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------


def setup_logging(log_dir: Path) -> logging.Logger:
    """Create a logger that writes to both stdout and a timestamped file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"mail_cannon_{timestamp}.log"

    logger = logging.getLogger("mail_cannon")
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Log file: %s", log_file)
    return logger


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_config(path: Path) -> dict:
    with open(path) as f:
        config = json.load(f)

    required_keys = ["theseus_base_url", "api_key", "tags", "skus"]
    missing = [k for k in required_keys if k not in config]
    if missing:
        raise SystemExit(f"Config missing keys: {', '.join(missing)}")

    if len(config["skus"]) != 12:
        raise SystemExit(
            f"Config must list exactly 12 SKUs, got {len(config['skus'])}"
        )

    if config["api_key"] == "YOUR_API_KEY_HERE":
        raise SystemExit("Set your real API key in config.json before running.")

    return config


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------


def read_orders_csv(path: Path, sku_names: list[str], logger: logging.Logger) -> list[dict]:
    expected = ADDRESS_COLUMNS + sku_names

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        missing_headers = [c for c in expected if c not in headers]
        if missing_headers:
            raise SystemExit(
                f"CSV is missing columns: {', '.join(missing_headers)}"
            )

        rows = [r for r in reader if (r.get("email") or "").strip()]

    logger.info("Read %d rows from %s (skipped empty/non-data rows)", len(rows), path)
    return rows


# ---------------------------------------------------------------------------
# Order building
# ---------------------------------------------------------------------------


def build_order_payload(
    row: dict, sku_names: list[str], tags: list[str]
) -> dict:
    """Turn a CSV row into a Theseus API request body."""
    contents = []
    for sku in sku_names:
        qty = int(row[sku] or 0)
        if qty > 0:
            contents.append({"sku": sku, "quantity": qty})

    payload = {
        "warehouse_order": {
            "recipient_email": row["email"].strip(),
            "tags": tags,
        },
        "address": {
            "first_name": row["first_name"].strip(),
            "last_name": (row.get("last_name") or "").strip(),
            "line_1": row["line_1"].strip(),
            "line_2": (row.get("line_2") or "").strip(),
            "city": row["city"].strip(),
            "state": row["state"].strip(),
            "postal_code": row["postal_code"].strip(),
            "country": row["country"].strip(),
        },
        "contents": contents,
    }

    # Drop blank optional fields so they aren't sent as empty strings
    if not payload["address"]["last_name"]:
        del payload["address"]["last_name"]
    if not payload["address"]["line_2"]:
        del payload["address"]["line_2"]

    return payload


def validate_row(row: dict, row_num: int, sku_names: list[str]) -> list[str]:
    """Return a list of validation error strings (empty = valid)."""
    errors = []
    for col in REQUIRED_ADDRESS_COLUMNS:
        if not (row.get(col) or "").strip():
            errors.append(f"Row {row_num}: missing required field '{col}'")

    # Check that at least one SKU has qty > 0
    has_items = False
    for sku in sku_names:
        raw = (row.get(sku) or "").strip()
        if raw:
            try:
                qty = int(raw)
                if qty < 0:
                    errors.append(
                        f"Row {row_num}: negative quantity in '{sku}'"
                    )
                if qty > 0:
                    has_items = True
            except ValueError:
                errors.append(
                    f"Row {row_num}: non-integer value '{raw}' in '{sku}'"
                )

    if not has_items:
        errors.append(f"Row {row_num}: no SKUs with quantity > 0")

    return errors


# ---------------------------------------------------------------------------
# API interaction
# ---------------------------------------------------------------------------


def create_order(
    base_url: str,
    api_key: str,
    payload: dict,
    logger: logging.Logger,
) -> dict:
    """POST to Theseus and return the JSON response."""
    url = f"{base_url.rstrip('/')}/api/v1/warehouse_orders"
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "mail-cannon/1.0",
        },
    )

    logger.debug("POST %s", url)
    logger.debug("Request body: %s", json.dumps(payload, indent=2))

    resp = urllib.request.urlopen(req, timeout=120)
    resp_bytes = resp.read()
    body = json.loads(resp_bytes) if resp_bytes else {}

    logger.debug("Response status: %d", resp.status)
    logger.debug("Response body: %s", json.dumps(body, indent=2))

    return body


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(csv_path: Path, config_path: Path, dry_run: bool = False):
    log_dir = Path(__file__).resolve().parent / "logs"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("mail-cannon starting")
    logger.info("CSV:      %s", csv_path)
    logger.info("Config:   %s", config_path)
    logger.info("Dry run:  %s", dry_run)
    logger.info("=" * 60)

    config = load_config(config_path)
    sku_names = config["skus"]
    tags = config["tags"]
    base_url = config["theseus_base_url"]

    rows = read_orders_csv(csv_path, sku_names, logger)

    # ---- Validate all rows first ----
    all_errors = []
    for idx, row in enumerate(rows, start=2):  # row 1 is header
        all_errors.extend(validate_row(row, idx, sku_names))

    if all_errors:
        for err in all_errors:
            logger.error(err)
        raise SystemExit(
            f"Validation failed with {len(all_errors)} error(s). Fix the CSV and retry."
        )

    logger.info("All %d rows passed validation.", len(rows))

    if dry_run:
        for idx, row in enumerate(rows, start=2):
            payload = build_order_payload(row, sku_names, tags)
            logger.info(
                "[DRY RUN] Row %d (%s): would send %d SKU line(s)",
                idx,
                row["email"].strip(),
                len(payload["contents"]),
            )
            logger.debug(
                "[DRY RUN] Row %d payload: %s",
                idx,
                json.dumps(payload, indent=2),
            )
        logger.info("Dry run complete. No orders were created.")
        return

    # ---- Send orders ----
    api_key = config["api_key"]

    succeeded = 0
    failed = 0
    results = []

    for idx, row in enumerate(rows, start=2):
        email = row["email"].strip()
        payload = build_order_payload(row, sku_names, tags)
        logger.info(
            "Row %d/%d | %s | %d SKU line(s) | sending...",
            idx - 1,
            len(rows),
            email,
            len(payload["contents"]),
        )

        try:
            resp_body = create_order(base_url, api_key, payload, logger)
            order_id = resp_body.get("id") or resp_body.get("hc_id") or "unknown"
            logger.info(
                "Row %d | SUCCESS | order_id=%s | email=%s",
                idx,
                order_id,
                email,
            )
            succeeded += 1
            results.append(
                {"row": idx, "email": email, "status": "success", "order_id": order_id, "response": resp_body}
            )
        except urllib.error.HTTPError as exc:
            error_body = {}
            try:
                error_body = json.loads(exc.read().decode())
            except Exception:
                error_body = {"raw": str(exc)}

            logger.error(
                "Row %d | FAILED  | email=%s | HTTP %d | %s",
                idx,
                email,
                exc.code,
                json.dumps(error_body),
            )
            failed += 1
            results.append(
                {
                    "row": idx,
                    "email": email,
                    "status": "failed",
                    "http_status": exc.code,
                    "error": error_body,
                }
            )
        except urllib.error.URLError as exc:
            logger.error(
                "Row %d | FAILED  | email=%s | %s",
                idx,
                email,
                str(exc.reason),
            )
            failed += 1
            results.append(
                {"row": idx, "email": email, "status": "failed", "error": str(exc.reason)}
            )

        # Small delay to avoid hammering the API
        time.sleep(0.5)

    # ---- Summary ----
    logger.info("=" * 60)
    logger.info("COMPLETE: %d succeeded, %d failed, %d total", succeeded, failed, len(rows))
    logger.info("=" * 60)

    # Write a JSON results file alongside the log
    results_file = log_dir / f"mail_cannon_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_results.json"
    with open(results_file, "w") as f:
        json.dump(
            {
                "run_at": datetime.now(timezone.utc).isoformat(),
                "csv": str(csv_path),
                "total": len(rows),
                "succeeded": succeeded,
                "failed": failed,
                "orders": results,
            },
            f,
            indent=2,
        )
    logger.info("Results written to %s", results_file)

    if failed:
        raise SystemExit(f"{failed} order(s) failed. Check the log for details.")


def main():
    parser = argparse.ArgumentParser(
        description="Batch-create Theseus warehouse orders from a CSV."
    )
    parser.add_argument("csv", type=Path, help="Path to the orders CSV file")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).resolve().parent / "config.json",
        help="Path to config.json (default: ./config.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and log payloads without sending any requests",
    )
    args = parser.parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV file not found: {args.csv}")
    if not args.config.exists():
        raise SystemExit(f"Config file not found: {args.config}")

    run(args.csv, args.config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
