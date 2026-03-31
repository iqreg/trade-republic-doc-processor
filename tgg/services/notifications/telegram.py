from __future__ import annotations

import os


class TelegramNotifier:
    def __init__(self) -> None:
        self.enabled = os.getenv("ENABLE_TELEGRAM", "false").lower() == "true"
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    def send(self, message: str) -> bool:
        if not self.enabled or not self.token or not self.chat_id:
            return False
        try:
            import httpx  # local import keeps module import-safe in minimal environments
        except Exception:
            return False

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(url, json={"chat_id": self.chat_id, "text": message})
            return resp.status_code == 200
