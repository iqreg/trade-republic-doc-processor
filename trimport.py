#!/usr/bin/env python3
import argparse
import csv
import hashlib
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence, Tuple


DATE_PATTERN = re.compile(r"\b(\d{1,2}\.\d{1,2}\.\d{4})\b")
DATE_WORD_PATTERN = re.compile(
    r"\b(\d{1,2})\s+([A-Za-zÄÖÜäöüß\.]+)\s+(\d{4})\b",
    re.IGNORECASE,
)
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
    "handel": "trade",
}

MONTH_MAP = {
    "jan": 1,
    "januar": 1,
    "feb": 2,
    "februar": 2,
    "mär": 3,
    "märz": 3,
    "mar": 3,
    "maerz": 3,
    "apr": 4,
    "april": 4,
    "mai": 5,
    "jun": 6,
    "juni": 6,
    "jul": 7,
    "juli": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "okt": 10,
    "oktober": 10,
    "nov": 11,
    "november": 11,
    "dez": 12,
    "dezember": 12,
}


@dataclass
class ParseResult:
    transactions: List["ParsedTransaction"]
    section_found: bool
    page_texts: List[str]


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


def normalize_word_month(month_value: str) -> Optional[int]:
    cleaned = month_value.strip(".").lower()
    return MONTH_MAP.get(cleaned)


def extract_date(text: str) -> Tuple[Optional[str], str]:
    match = DATE_PATTERN.search(text)
    if match:
        date_iso = normalize_date(match.group(1))
        stripped = text[: match.start()] + text[match.end() :]
        return date_iso, stripped.strip()

    match = DATE_WORD_PATTERN.search(text)
    if match:
        day = int(match.group(1))
        month = normalize_word_month(match.group(2))
        year = int(match.group(3))
        if month:
            date_iso = datetime(year, month, day).date().isoformat()
            stripped = text[: match.start()] + text[match.end() :]
            return date_iso, stripped.strip()

    return None, text.strip()


def extract_amounts(text: str) -> List[float]:
    matches = AMOUNT_PATTERN.findall(text)
    return [parse_amount(match) for match in matches]


def extract_quantity(description: str, isin_match: Optional[re.Match]) -> Optional[float]:
    search_text = description
    if isin_match:
        search_text = description[isin_match.end() :]
    matches = re.findall(r"\b(\d+(?:[.,]\d+)?)\b", search_text)
    if not matches:
        return None
    candidate = matches[0]
    if "," in candidate:
        return float(candidate.replace(",", "."))
    return float(candidate)


def normalize_description(description: str) -> str:
    lowered = description.lower()
    for prefix in ("buy trade", "sell trade", "buy", "sell"):
        if lowered.startswith(prefix):
            return description[len(prefix) :].strip()
    return description


def build_txn_hash(parts: Sequence[Optional[str]]) -> str:
    raw = "|".join([p or "" for p in parts])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_transaction_line(line: str, source_pdf: str) -> Optional[ParsedTransaction]:
    date_iso, remainder = extract_date(line)
    if not date_iso:
        return None

    lower_line = remainder.lower()
    txn_type_key = None
    type_match = None
    for key in sorted(TYPE_MAP, key=len, reverse=True):
        match = re.search(rf"\b{re.escape(key)}\b", lower_line)
        if match:
            txn_type_key = key
            type_match = match
            break
    if not txn_type_key or not type_match:
        return None

    txn_type = TYPE_MAP[txn_type_key]
    description = remainder[type_match.end() :].strip()

    if txn_type_key == "handel":
        desc_lower = description.lower()
        if "buy" in desc_lower:
            txn_type = "buy"
        elif "sell" in desc_lower:
            txn_type = "sell"
        else:
            txn_type = "transfer"

    amounts = extract_amounts(remainder)
    if not amounts:
        return None

    balance = None
    amount_in = None
    amount_out = None

    if len(amounts) >= 3:
        amount_in = amounts[-3]
        amount_out = amounts[-2]
        balance = amounts[-1]
    elif len(amounts) == 2:
        txn_amount = amounts[0]
        balance = amounts[-1]
        if txn_type == "buy":
            amount_out = txn_amount
        elif txn_type == "sell":
            amount_in = txn_amount
        elif txn_amount < 0:
            amount_out = abs(txn_amount)
        else:
            amount_in = txn_amount
    else:
        balance = amounts[-1]

    amount_start = AMOUNT_PATTERN.search(remainder)
    description_only = description
    if amount_start:
        description_only = remainder[: amount_start.start()].strip()
        if type_match:
            description_only = remainder[type_match.end() : amount_start.start()].strip()

    description_only = normalize_description(description_only)
    isin_match = ISIN_PATTERN.search(description_only)
    isin_value = isin_match.group(1) if isin_match else None

    instrument_name = description_only
    if isin_match:
        instrument_name = description_only[: isin_match.start()].strip()
    instrument_name = instrument_name or None

    quantity = extract_quantity(description_only, isin_match)

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


def is_table_header(line: str) -> bool:
    lower = line.lower()
    return all(
        token in lower
        for token in (
            "datum",
            "typ",
            "beschreibung",
            "zahlungseingang",
            "zahlungsausgang",
            "saldo",
        )
    )


def extract_transactions_from_text(text: str, source_pdf: str) -> Tuple[List[ParsedTransaction], bool]:
    transactions: List[ParsedTransaction] = []
    lines = text.splitlines()
    section_found = any("umsatzübersicht" in line.lower() for line in lines)
    in_table = False
    buffer: List[str] = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        lower = stripped.lower()

        if "umsatzübersicht" in lower:
            in_table = False
            buffer.clear()
            continue

        if is_table_header(stripped):
            in_table = True
            buffer.clear()
            continue

        if not in_table:
            continue

        buffer.append(stripped)
        combined = " ".join(buffer)
        parsed = parse_transaction_line(combined, source_pdf)
        if parsed:
            transactions.append(parsed)
            buffer.clear()

    return transactions, section_found


def parse_pdf(pdf_path: str) -> ParseResult:
    parsed: List[ParsedTransaction] = []
    page_texts: List[str] = []
    section_found = False
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            page_texts.append(text)
            page_transactions, page_section_found = extract_transactions_from_text(text, pdf_path)
            parsed.extend(page_transactions)
            section_found = section_found or page_section_found
    return ParseResult(transactions=parsed, section_found=section_found, page_texts=page_texts)


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


def write_debug_dump(debug_dump: str, pdf_path: str, page_texts: Sequence[str]) -> None:
    os.makedirs(debug_dump, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    for index, text in enumerate(page_texts, start=1):
        filename = f"{base_name}_page_{index}.txt"
        output_path = os.path.join(debug_dump, filename)
        with open(output_path, "w", encoding="utf-8") as handle:
            handle.write(text)


def scan_folder(folder: str, db_path: str, debug_dump: Optional[str]) -> int:
    init_db(db_path)
    inserted = 0
    for root, _, files in os.walk(folder):
        for filename in files:
            if not filename.lower().endswith(".pdf"):
                continue
            pdf_path = os.path.join(root, filename)
            checksum = compute_checksum(pdf_path)
            document_id = upsert_document(db_path, pdf_path, checksum)
            result = parse_pdf(pdf_path)
            if debug_dump:
                write_debug_dump(debug_dump, pdf_path, result.page_texts)
            inserted_count = insert_transactions(db_path, document_id, result.transactions)
            inserted += inserted_count
            print(
                f"{pdf_path}: found {len(result.transactions)} transactions, "
                f"inserted {inserted_count} new."
            )
            if len(result.transactions) == 0:
                print(
                    f"{pdf_path}: section 'Umsatzübersicht' found: "
                    f"{'yes' if result.section_found else 'no'}."
                )
    return inserted


def export_rows(db_path: str, output_path: str, fmt: str) -> int:
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
                source_pdf
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
    return len(rows)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trade Republic PDF import/export tool.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Scan folder for Trade Republic PDFs.")
    scan_parser.add_argument("--folder", required=True, help="Folder containing PDF documents.")
    scan_parser.add_argument("--db", default="trade_republic.db", help="SQLite database path.")
    scan_parser.add_argument(
        "--debug-dump",
        default=None,
        help="Dump extracted page text to this folder for debugging.",
    )

    export_parser = subparsers.add_parser("export", help="Export parsed transactions.")
    export_parser.add_argument("--format", choices=["csv", "xlsx"], required=True)
    export_parser.add_argument("--out", required=True, help="Output file path.")
    export_parser.add_argument("--db", default="trade_republic.db", help="SQLite database path.")

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    if args.command == "scan":
        inserted = scan_folder(args.folder, args.db, args.debug_dump)
        print(f"Inserted {inserted} transactions into {args.db}.")
    elif args.command == "export":
        exported = export_rows(args.db, args.out, args.format)
        print(f"Exported {exported} rows to {args.out}.")


if __name__ == "__main__":
    main()
