#!/usr/bin/env python3
import argparse
import csv
import hashlib
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple


DATE_PATTERN = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")
ISIN_PATTERN = re.compile(r"\b([A-Z]{2}[A-Z0-9]{10})\b")
AMOUNT_PATTERN = re.compile(r"(-?\d{1,3}(?:\.\d{3})*,\d{2})")

TYPE_MAP = {
    "kauf": "buy",
    "verkauf": "sell",
    "übertrag": "transfer",
    "einzahlung": "transfer",
    "auszahlung": "transfer",
    "dividende": "transfer",
    "steuer": "transfer",
    "gebühr": "transfer",
    "transfer": "transfer",
}


@dataclass
class ParsedTransaction:
    date: str
    txn_type: str
    isin: Optional[str]
    instrument_name: Optional[str]
    quantity: Optional[float]
    amount_in: Optional[float]
    amount_out: Optional[float]
    balance: Optional[float]
    source_pdf: str
    txn_hash: str


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_pdf TEXT NOT NULL,
    checksum TEXT NOT NULL UNIQUE,
    scanned_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    type TEXT NOT NULL,
    isin TEXT,
    instrument_name TEXT,
    quantity REAL,
    amount_in REAL,
    amount_out REAL,
    balance REAL,
    source_pdf TEXT NOT NULL,
    txn_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    FOREIGN KEY(document_id) REFERENCES documents(id)
);
"""


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def parse_amount(value: str) -> float:
    normalized = value.replace(".", "").replace(",", ".")
    return float(normalized)


def normalize_date(date_value: str) -> str:
    return datetime.strptime(date_value, "%d.%m.%Y").date().isoformat()


def build_txn_hash(parts: Sequence[Optional[str]]) -> str:
    raw = "|".join([p or "" for p in parts])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_transaction_line(line: str, source_pdf: str) -> Optional[ParsedTransaction]:
    date_match = DATE_PATTERN.search(line)
    if not date_match:
        return None

    lower_line = line.lower()
    txn_type_key = None
    for key in sorted(TYPE_MAP, key=len, reverse=True):
        if re.search(rf"\b{re.escape(key)}\b", lower_line):
            txn_type_key = key
            break
    if not txn_type_key:
        return None

    txn_type = TYPE_MAP[txn_type_key]
    date_iso = normalize_date(date_match.group(1))

    isin_match = ISIN_PATTERN.search(line)
    isin_value = isin_match.group(1) if isin_match else None

    instrument_name = None
    if isin_match:
        before_isin = line[: isin_match.start()].strip()
        tokens = re.split(r"\s+", before_isin)
        try:
            type_index = [t.lower() for t in tokens].index(txn_type_key)
        except ValueError:
            type_index = None
        if type_index is not None and type_index + 1 < len(tokens):
            instrument_name = " ".join(tokens[type_index + 1 :]).strip() or None

    amounts = [parse_amount(match) for match in AMOUNT_PATTERN.findall(line)]

    quantity = None
    quantity_source = line
    if isin_match:
        quantity_source = line[isin_match.end() :]
    quantity_match = re.search(r"\b(\d+(?:,\d+)?)\b", quantity_source)
    if quantity_match:
        candidate = quantity_match.group(1)
        if "," in candidate:
            quantity = float(candidate.replace(",", "."))
        else:
            quantity = float(candidate)

    amount_in = None
    amount_out = None
    balance = None

    if amounts:
        if len(amounts) >= 2:
            balance = amounts[-1]
            txn_amount = amounts[0]
        else:
            txn_amount = amounts[0]

        if txn_type == "buy":
            amount_out = txn_amount
        elif txn_type == "sell":
            amount_in = txn_amount
        else:
            if txn_amount < 0:
                amount_out = abs(txn_amount)
            else:
                amount_in = txn_amount

    txn_hash = build_txn_hash(
        [
            date_iso,
            txn_type,
            isin_value,
            instrument_name,
            str(quantity) if quantity is not None else None,
            str(amount_in) if amount_in is not None else None,
            str(amount_out) if amount_out is not None else None,
            str(balance) if balance is not None else None,
            source_pdf,
        ]
    )

    return ParsedTransaction(
        date=date_iso,
        txn_type=txn_type,
        isin=isin_value,
        instrument_name=instrument_name,
        quantity=quantity,
        amount_in=amount_in,
        amount_out=amount_out,
        balance=balance,
        source_pdf=source_pdf,
        txn_hash=txn_hash,
    )


def extract_transactions_from_text(text: str, source_pdf: str) -> List[ParsedTransaction]:
    transactions: List[ParsedTransaction] = []
    in_section = False

    for line in text.splitlines():
        if "umsatzübersicht" in line.lower():
            in_section = True
            continue
        if not in_section:
            continue
        if not line.strip():
            continue
        parsed = parse_transaction_line(line, source_pdf)
        if parsed:
            transactions.append(parsed)

    return transactions


def parse_pdf(pdf_path: str) -> List[ParsedTransaction]:
    parsed: List[ParsedTransaction] = []
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            parsed.extend(extract_transactions_from_text(text, pdf_path))
    return parsed


def compute_checksum(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def upsert_document(db_path: str, source_pdf: str, checksum: str) -> int:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO documents (source_pdf, checksum, scanned_at) VALUES (?, ?, ?)",
            (source_pdf, checksum, datetime.utcnow().isoformat(timespec="seconds")),
        )
        row = conn.execute("SELECT id FROM documents WHERE checksum = ?", (checksum,)).fetchone()
        conn.commit()
        return int(row[0])


def insert_transactions(db_path: str, document_id: int, transactions: Sequence[ParsedTransaction]) -> int:
    if not transactions:
        return 0

    with sqlite3.connect(db_path) as conn:
        now = datetime.utcnow().isoformat(timespec="seconds")
        conn.executemany(
            """
            INSERT OR IGNORE INTO transactions (
                document_id,
                date,
                type,
                isin,
                instrument_name,
                quantity,
                amount_in,
                amount_out,
                balance,
                source_pdf,
                txn_hash,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    document_id,
                    txn.date,
                    txn.txn_type,
                    txn.isin,
                    txn.instrument_name,
                    txn.quantity,
                    txn.amount_in,
                    txn.amount_out,
                    txn.balance,
                    txn.source_pdf,
                    txn.txn_hash,
                    now,
                )
                for txn in transactions
            ],
        )
        conn.commit()
        return conn.total_changes


def scan_folder(folder: str, db_path: str) -> int:
    init_db(db_path)
    inserted = 0
    for root, _, files in os.walk(folder):
        for filename in files:
            if not filename.lower().endswith(".pdf"):
                continue
            pdf_path = os.path.join(root, filename)
            checksum = compute_checksum(pdf_path)
            document_id = upsert_document(db_path, pdf_path, checksum)
            transactions = parse_pdf(pdf_path)
            inserted += insert_transactions(db_path, document_id, transactions)
    return inserted


def export_rows(db_path: str, output_path: str, fmt: str) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            SELECT
                date,
                type,
                isin,
                instrument_name,
                quantity,
                amount_in,
                amount_out,
                balance,
                source_pdf,
                txn_hash
            FROM transactions
            ORDER BY date, id
            """,
        )
        rows = cursor.fetchall()

    headers = [
        "date",
        "type",
        "isin",
        "instrument_name",
        "quantity",
        "amount_in",
        "amount_out",
        "balance",
        "source_pdf",
        "txn_hash",
    ]

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if fmt == "csv":
        with open(output_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
    elif fmt == "xlsx":
        from openpyxl import Workbook

        workbook = Workbook()
        sheet = workbook.active
        sheet.append(headers)
        for row in rows:
            sheet.append(list(row))
        workbook.save(output_path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trade Republic PDF import/export tool.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Scan folder for Trade Republic PDFs.")
    scan_parser.add_argument("--folder", required=True, help="Folder containing PDF documents.")
    scan_parser.add_argument("--db", default="trade_republic.db", help="SQLite database path.")

    export_parser = subparsers.add_parser("export", help="Export parsed transactions.")
    export_parser.add_argument("--format", choices=["csv", "xlsx"], required=True)
    export_parser.add_argument("--out", required=True, help="Output file path.")
    export_parser.add_argument("--db", default="trade_republic.db", help="SQLite database path.")

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    if args.command == "scan":
        inserted = scan_folder(args.folder, args.db)
        print(f"Inserted {inserted} transactions into {args.db}.")
    elif args.command == "export":
        export_rows(args.db, args.out, args.format)
        print(f"Exported transactions to {args.out}.")


if __name__ == "__main__":
    main()
