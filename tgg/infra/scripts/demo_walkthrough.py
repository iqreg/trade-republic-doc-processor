from __future__ import annotations

import os
from pprint import pprint


def main() -> None:
    try:
        from fastapi.testclient import TestClient
        from tgg.apps.api.main import app
    except Exception as exc:
        print(f"Demo skipped: missing dependency ({exc}). Run `make native-setup` first.")
        return

    os.environ.setdefault("DATABASE_URL", "sqlite:///./tgg_demo.db")
    with TestClient(app) as client:
        print("== /health ==")
        pprint(client.get("/health").json())

        print("== /run-once ==")
        pprint(client.post("/run-once").json())

        print("== /status ==")
        pprint(client.get("/status").json())


if __name__ == "__main__":
    main()
