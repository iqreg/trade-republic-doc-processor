# Trade Republic Doc Processor (MVP)

Parse Trade Republic PDF documents into a SQLite database and export transactions (primary output: Excel).

## Setup

```bash
pip install -r requirements.txt
```

## Usage

### Scan PDFs into SQLite

```bash
./trimport scan --folder path/to/pdfs --db trade_republic.db
```

To debug extraction, dump pdfplumber text per page:

```bash
./trimport scan --folder path/to/pdfs --db trade_republic.db --debug-dump dumps/
```

The scanner looks for `Umsatzübersicht` sections and extracts transactions with the following fields:
`date`, `type`, `isin`, `instrument_name`, `quantity`, `amount_in`, `amount_out`, `balance`, `source_pdf`.

### Export to XLSX (primary) or CSV

```bash
./trimport export --format csv --out export.csv --db trade_republic.db
./trimport export --format xlsx --out export.xlsx --db trade_republic.db
```

## Tests

```bash
pytest
```
