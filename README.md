# Trade Republic Doc Processor (MVP)

Parse Trade Republic PDF documents into a SQLite database and export the parsed rows.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

### Parse PDFs into SQLite

```bash
python main.py parse path/to/document.pdf --db trade_republic.db
```

This creates a `transactions` table and stores each non-empty line with detected metadata (date, ISIN, amount, currency).

### Export to CSV or JSON

```bash
python main.py export --db trade_republic.db --format csv --output export.csv
python main.py export --db trade_republic.db --format json --output export.json
```

Optionally limit exported rows:

```bash
python main.py export --db trade_republic.db --format csv --output export.csv --limit 100
```
