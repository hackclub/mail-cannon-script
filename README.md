# mail-cannon

Batch warehouse order creator for [Theseus](https://github.com/hackclub/theseus) (Hack Club's mail system). Reads a CSV of recipients with per-SKU quantities, creates a warehouse order on Theseus for each row, and writes detailed logs for every request/response.

## Requirements

- Python 3.10+
- No external dependencies (uses stdlib `urllib`)

## Setup

1. Copy the example config and add your API key:

```
cp config.example.json config.json
```

2. Edit `config.json`:
   - Set `api_key` to your Theseus API key
   - Set `tags` to the tag(s) for this batch
   - Set `skus` to the 12 SKU codes (order matters -- they map to CSV columns)

## CSV format

The CSV must have these columns in the header row:

```
first_name,last_name,email,line_1,line_2,city,state,postal_code,country,<SKU_1>,<SKU_2>,...,<SKU_12>
```

- `first_name`, `email`, `line_1`, `city`, `state`, `postal_code`, `country` are required.
- `last_name` and `line_2` are optional.
- Each SKU column header must exactly match a SKU string in `config.json`.
- SKU column values are integer quantities. Use `0` to skip a SKU for that row.
- Rows with an empty `email` field are skipped (handles trailing blank/totals rows).
- Extra columns (e.g. `attendee_count`) are ignored.

See `sample.csv` for a working example.

## Usage

Validate without sending anything:

```
python3 mail_cannon.py orders.csv --dry-run
```

Send all orders:

```
python3 mail_cannon.py orders.csv
```

Specify a different config file:

```
python3 mail_cannon.py orders.csv --config /path/to/config.json
```

## Output

All output goes to `logs/`:

- `mail_cannon_<timestamp>.log` -- human-readable log with every request/response at DEBUG level, summaries at INFO level.
- `mail_cannon_<timestamp>_results.json` -- machine-readable JSON with per-order status, order IDs, and full API responses.

## How it works

1. Loads config and validates the API key and SKU list.
2. Reads the CSV and validates every row (required fields, integer quantities, at least one SKU > 0).
3. If any row fails validation, the entire batch is aborted before sending anything.
4. Sends a `POST /api/v1/warehouse_orders` request to Theseus for each row.
5. Each order is automatically dispatched to Zenventory (the warehouse fulfillment backend).
6. Logs success/failure for each row, then writes a summary.

## API details

- Endpoint: `POST /api/v1/warehouse_orders`
- Auth: `Bearer <api_key>` header
- Theseus normalizes country names and state abbreviations automatically.
- Blocked destinations: IR, PS, CU, KP, RU.
- A 0.5s delay is inserted between requests to avoid rate limiting.
- Request timeout is 120s (the server can be slow on large orders).
