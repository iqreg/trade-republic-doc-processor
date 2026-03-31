from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .models import Base


DEFAULT_NATIVE_DB = "sqlite:///./tgg_native.db"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_NATIVE_DB)

if DATABASE_URL.startswith("sqlite:///"):
    db_path = DATABASE_URL.replace("sqlite:///", "")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(DATABASE_URL, future=True)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def session() -> Session:
    return Session(bind=engine)
