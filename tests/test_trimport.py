import os
import sqlite3
from pathlib import Path

from trimport import (
    extract_transactions_from_text,
    init_db,
    insert_transactions,
    parse_transaction_line,
    upsert_document,
)


def test_parse_transaction_line_buy():
    line = "01.02.2024 Kauf Example AG DE0001234567 10 1.234,56 5.000,00"
    txn = parse_transaction_line(line, "sample.pdf")
    assert txn is not None
    assert txn.date == "2024-02-01"
    assert txn.txn_type == "buy"
    assert txn.isin == "DE0001234567"
    assert txn.instrument_name == "Example AG"
    assert txn.amount_out == 1234.56
    assert txn.balance == 5000.0


def test_extract_transactions_from_text_filters_section():
    text = "Header\nUmsatzübersicht\n01.03.2024 Verkauf Demo SE DE0009999999 5 2.000,00 7.000,00"
    txns = extract_transactions_from_text(text, "sample.pdf")
    assert len(txns) == 1
    assert txns[0].txn_type == "sell"


def test_insert_transactions_deduplicates(tmp_path: Path):
    db_path = tmp_path / "test.db"
    init_db(str(db_path))
    doc_id = upsert_document(str(db_path), "sample.pdf", "checksum")

    line = "01.02.2024 Kauf Example AG DE0001234567 10 1.234,56 5.000,00"
    txn = parse_transaction_line(line, "sample.pdf")
    inserted_first = insert_transactions(str(db_path), doc_id, [txn])
    inserted_second = insert_transactions(str(db_path), doc_id, [txn])

    assert inserted_first == 1
    assert inserted_second == 0

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert count == 1
