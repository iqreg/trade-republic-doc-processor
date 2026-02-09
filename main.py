import argparse
import csv
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple

import pdfplumber

DATE_PATTERN = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")
AMOUNT_PATTERN = re.compile(r"(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*(EUR|USD|CHF|GBP)?")
ISIN_PATTERN = re.compile(r"\b([A-Z]{2}[A-Z0-9]{10})\b")


@dataclass
class ParsedLine:
    source_file: str
    page: int
    line_no: int
    raw_line: str
    date: Optional[str]
    isin: Optional[str]
    amount: Optional[str]
    currency: Optional[str]


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    page INTEGER NOT NULL,
    line_no INTEGER NOT NULL,
    raw_line TEXT NOT NULL,
    date TEXT,
    isin TEXT,
    amount TEXT,
    currency TEXT,
    created_at TEXT NOT NULL
);
"""


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(SCHEMA_SQL)
        conn.commit()


def parse_line(source_file: str, page: int, line_no: int, line: str) -> ParsedLine:
    date_match = DATE_PATTERN.search(line)
    amount_match = AMOUNT_PATTERN.search(line)
    isin_match = ISIN_PATTERN.search(line)

    date_value = date_match.group(1) if date_match else None
    amount_value = amount_match.group(1) if amount_match else None
    currency_value = amount_match.group(2) if amount_match and amount_match.group(2) else None
    isin_value = isin_match.group(1) if isin_match else None

    return ParsedLine(
        source_file=source_file,
        page=page,
        line_no=line_no,
        raw_line=line.strip(),
        date=date_value,
        isin=isin_value,
        amount=amount_value,
        currency=currency_value,
    )


def parse_pdf(pdf_path: str) -> List[ParsedLine]:
    parsed_lines: List[ParsedLine] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            for line_no, line in enumerate(text.splitlines(), start=1):
                if not line.strip():
                    continue
                parsed_lines.append(parse_line(pdf_path, page_index, line_no, line))
    return parsed_lines


def insert_lines(db_path: str, lines: Sequence[ParsedLine]) -> None:
    if not lines:
        return
    with sqlite3.connect(db_path) as conn:
        now = datetime.utcnow().isoformat(timespec="seconds")
        conn.executemany(
            """
            INSERT INTO transactions (
                source_file,
                page,
                line_no,
                raw_line,
                date,
                isin,
                amount,
                currency,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    line.source_file,
                    line.page,
                    line.line_no,
                    line.raw_line,
                    line.date,
                    line.isin,
                    line.amount,
                    line.currency,
                    now,
                )
                for line in lines
            ],
        )
        conn.commit()


def export_rows(
    db_path: str,
    output_path: str,
    fmt: str,
    limit: Optional[int],
) -> None:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            "SELECT source_file, page, line_no, raw_line, date, isin, amount, currency, created_at FROM transactions ORDER BY id",
        )
        rows = cursor.fetchall()
        if limit is not None:
            rows = rows[:limit]

    headers = [
        "source_file",
        "page",
        "line_no",
        "raw_line",
        "date",
        "isin",
        "amount",
        "currency",
        "created_at",
    ]

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if fmt == "csv":
        with open(output_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
    elif fmt == "json":
        payload = [dict(zip(headers, row)) for row in rows]
        with open(output_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse Trade Republic PDFs into SQLite and export.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_parser = subparsers.add_parser("parse", help="Parse PDF files into SQLite.")
    parse_parser.add_argument("pdfs", nargs="+", help="Path(s) to Trade Republic PDF files.")
    parse_parser.add_argument("--db", default="trade_republic.db", help="SQLite database path.")

    export_parser = subparsers.add_parser("export", help="Export parsed rows to CSV/JSON.")
    export_parser.add_argument("--db", default="trade_republic.db", help="SQLite database path.")
    export_parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Export format.")
    export_parser.add_argument("--output", required=True, help="Output file path.")
    export_parser.add_argument("--limit", type=int, default=None, help="Limit number of exported rows.")

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    if args.command == "parse":
        init_db(args.db)
        all_lines: List[ParsedLine] = []
        for pdf_path in args.pdfs:
            parsed = parse_pdf(pdf_path)
            all_lines.extend(parsed)
        insert_lines(args.db, all_lines)
        print(f"Inserted {len(all_lines)} lines into {args.db}.")
    elif args.command == "export":
        export_rows(args.db, args.output, args.format, args.limit)
        print(f"Exported rows to {args.output}.")


if __name__ == "__main__":
    main()
