from __future__ import annotations

from pathlib import Path


def load_env_file() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    env_path = Path(".env")
    if env_path.exists():
        load_dotenv(env_path, override=False)
