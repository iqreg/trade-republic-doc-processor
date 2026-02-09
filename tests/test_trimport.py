import sqlite3
from pathlib import Path

import trimport
from trimport import (
    ParseResult,
    extract_transactions_from_text,
    init_db,
    insert_transactions,
    parse_transaction_line,
    scan_folder,
    upsert_document,
)


def test_parse_transaction_line_buy():
    line = "01.02.2024 Kauf Example AG DE0001234567 10 0,00 1.234,56 5.000,00"
    txn = parse_transaction_line(line, "sample.pdf")
    assert txn is not None
    assert txn.date == "2024-02-01"
    assert txn.txn_type == "buy"
    assert txn.isin == "DE0001234567"
    assert txn.instrument_name == "Example AG"
    assert txn.amount_out == 1234.56
    assert txn.balance == 5000.0


def test_extract_transactions_from_text_filters_section():
    text = "Header\nUmsatzübersicht\nDATUM TYP BESCHREIBUNG ZAHLUNGSEINGANG ZAHLUNGSAUSGANG SALDO\n"
    text += "01.03.2024 Verkauf Demo SE DE0009999999 5 2.000,00 0,00 7.000,00"
    txns, section_found = extract_transactions_from_text(text, "sample.pdf")
    assert section_found is True
    assert len(txns) == 1
    assert txns[0].txn_type == "sell"


def test_fixture_parsing_layout():
    fixture_path = Path(__file__).parent / "fixtures" / "umsatz_sample.txt"
    text = fixture_path.read_text(encoding="utf-8")
    txns, section_found = extract_transactions_from_text(text, "sample.pdf")

    assert section_found is True
    assert len(txns) >= 1

    first = txns[0]
    assert first.date == "2025-09-22"
    assert first.txn_type == "buy"
    assert first.isin == "DE0001234567"
    assert first.quantity == 10.0
    assert first.amount_out == 1234.56
    assert first.balance == 5000.0


def test_insert_transactions_deduplicates(tmp_path: Path):
    db_path = tmp_path / "test.db"
    init_db(str(db_path))
    doc_id = upsert_document(str(db_path), "sample.pdf", "checksum")

    line = "01.02.2024 Kauf Example AG DE0001234567 10 0,00 1.234,56 5.000,00"
    txn = parse_transaction_line(line, "sample.pdf")
    inserted_first = insert_transactions(str(db_path), doc_id, [txn])
    inserted_second = insert_transactions(str(db_path), doc_id, [txn])

    assert inserted_first == 1
    assert inserted_second == 0

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert count == 1


def test_scan_writes_debug_dump_on_empty(tmp_path: Path, monkeypatch):
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%EOF")

    def fake_parse_pdf(path: str) -> ParseResult:
        return ParseResult(transactions=[], section_found=False, page_texts=["page 1"])

    monkeypatch.setattr(trimport, "parse_pdf", fake_parse_pdf)

    debug_dump = tmp_path / "dump"
    scan_folder(str(tmp_path), str(tmp_path / "test.db"), str(debug_dump))

    dumped_files = list(debug_dump.glob("*.txt"))
    assert dumped_files
